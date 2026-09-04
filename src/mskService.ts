import { Kafka, Consumer, ConfigResourceTypes } from 'kafkajs';
import { generateAuthToken, generateAuthTokenFromRole } from 'aws-msk-iam-sasl-signer-js';

export interface MSKClusterConfig {
  name: string;
  region: string;
  /**
   * ARN da role a assumir. Ausente/vazio significa usar as credenciais que já
   * estão no ambiente (role já assumida via `aws sts assume-role`, instance
   * profile, ECS/EKS task role), sem novo AssumeRole.
   */
  roleArn?: string;
  brokers: string[];
}

/** Texto curto de como o cluster autentica, para tooltips e mensagens. */
export function describeAuthMode(config: MSKClusterConfig): string {
  return config.roleArn?.trim()
    ? `Assume Role: ${config.roleArn}`
    : 'Credenciais atuais (role já assumida no ambiente)';
}

/** Objeto/array quando o payload é JSON; string crua caso contrário. */
export type MSKPayload = unknown;

export interface MSKMessage {
  partition: number;
  offset: string;
  key: MSKPayload;
  value: MSKPayload;
  /** Data/hora legível no fuso local, derivada de `timestampMs`. */
  timestamp: string;
  /** Unix epoch em ms, como o broker devolve; usado para ordenar sem perder precisão. */
  timestampMs: string;
}

/**
 * Converte o unixtime em ms do Kafka para `YYYY-MM-DD HH:mm:ss.SSS ±HH:MM` no
 * fuso local, que é o formato que se lê direto no documento de mensagens.
 *
 * Timestamp ausente ou não-numérico volta como string vazia em vez de
 * "Invalid Date" — inclusive o -1 que o Kafka usa para NO_TIMESTAMP.
 */
export function formatTimestamp(raw: string | number | null | undefined): string {
  const ms = Number(raw);
  if (raw === null || raw === undefined || raw === '' || !Number.isFinite(ms) || ms < 0) return '';

  const date = new Date(ms);
  if (Number.isNaN(date.getTime())) return '';

  const pad = (value: number, size = 2) => String(Math.abs(value)).padStart(size, '0');

  // getTimezoneOffset() é minutos a subtrair do local para chegar em UTC: sinal invertido.
  const offsetMinutes = -date.getTimezoneOffset();
  const offset = `${offsetMinutes < 0 ? '-' : '+'}${pad(Math.trunc(offsetMinutes / 60))}:${pad(offsetMinutes % 60)}`;

  return (
    `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ` +
    `${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${pad(date.getMilliseconds(), 3)} ` +
    offset
  );
}

/**
 * Converte o payload em objeto quando ele é JSON, para o documento exibido não
 * ficar com o JSON escapado dentro de uma string.
 *
 * Só tenta parsear o que começa com `{` ou `[`: assim uma key como "0123" ou
 * "42" continua string, sem virar número e perder zeros à esquerda.
 */
export function parsePayload(buffer: Buffer | null | undefined): MSKPayload {
  if (!buffer) return '';

  const raw = buffer.toString();
  const trimmed = raw.trim();
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return raw;

  try {
    return JSON.parse(trimmed);
  } catch {
    return raw; // Payload não-JSON (Avro, Protobuf, texto truncado).
  }
}

export interface PartitionOffsets {
  partition: number;
  low: number;
  high: number;
}

/**
 * Converte uma duração em ms de config do Kafka para algo legível ("7 dias").
 *
 * `-1` é o valor que o Kafka usa para "sem limite" em retention.ms/segment.ms.
 */
export function formatDuration(raw: string | number | null | undefined): string {
  const ms = Number(raw);
  if (raw === null || raw === undefined || raw === '' || !Number.isFinite(ms)) return '';
  if (ms < 0) return 'ilimitado';
  if (ms === 0) return '0';

  const units: Array<[string, number]> = [
    ['dia(s)', 86400000],
    ['hora(s)', 3600000],
    ['minuto(s)', 60000],
    ['segundo(s)', 1000]
  ];

  for (const [label, size] of units) {
    if (ms >= size) {
      const value = ms / size;
      // Sem casa decimal quando é exato (7 dias), com uma casa quando não é.
      return `${Number.isInteger(value) ? value : value.toFixed(1)} ${label}`;
    }
  }

  return `${ms} ms`;
}

/** Tamanho em bytes de config do Kafka em formato legível; `-1` é sem limite. */
export function formatBytes(raw: string | number | null | undefined): string {
  const bytes = Number(raw);
  if (raw === null || raw === undefined || raw === '' || !Number.isFinite(bytes)) return '';
  if (bytes < 0) return 'ilimitado';

  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }

  return `${Number.isInteger(value) ? value : value.toFixed(1)} ${units[unit]}`;
}

export interface TopicPartitionMetadata {
  partition: number;
  leader: number;
  replicas: number[];
  /** In-sync replicas: menor que `replicas` indica partição sub-replicada. */
  isr: number[];
  low: number;
  high: number;
  /** high - low: mensagens ainda no log, já descontada a retenção. */
  messages: number;
}

export interface TopicMetadata {
  cluster: string;
  topic: string;
  partitions: number;
  /** Menor fator de replicação entre as partições. */
  replicationFactor: number;
  /** Partições em que ISR < replicas. */
  underReplicatedPartitions: number[];
  retention: {
    ms?: string;
    tempo?: string;
    bytes?: string;
    tamanho?: string;
  };
  cleanupPolicy?: string;
  minInSyncReplicas?: string;
  totalMessages: number;
  /** Configs do tópico que diferem do default do broker. */
  overrides: Record<string, string>;
  /** Configs mais consultadas, mesmo quando estão no valor default. */
  configsRelevantes: Record<string, string>;
  detalhePartições: TopicPartitionMetadata[];
}

export interface TruncateTopicResult {
  topic: string;
  /** Mensagens que existiam no log antes do truncate. */
  removed: number;
  /** Offset (high watermark) até onde cada partição foi apagada. */
  partitions: Array<{ partition: number; offset: number }>;
}

/** Configs sempre exibidas no relatório, mesmo quando estão no default. */
const RELEVANT_TOPIC_CONFIGS = [
  'retention.ms',
  'retention.bytes',
  'cleanup.policy',
  'min.insync.replicas',
  'max.message.bytes',
  'segment.ms',
  'segment.bytes',
  'compression.type',
  'delete.retention.ms',
  'message.timestamp.type'
];

export interface FetchMessagesResult {
  messages: MSKMessage[];
  /** Offsets lidos do broker antes de consumir (high watermark por partição). */
  offsets: PartitionOffsets[];
  /** Soma de (high - low): quantas mensagens o broker diz existir no log. */
  available: number;
  /** true quando o tempo acabou antes de alcançar o high watermark de todas as partições. */
  timedOut: boolean;
  /** Como as mensagens foram lidas: fetch direto nas partições ou consumer group. */
  mode: 'direct' | 'consumer-group';
  /** Só no modo consumer-group. */
  groupId?: string;
}

export interface FetchMessagesOptions {
  maxMessages?: number;
  timeoutMs?: number;
  /** Prefixo do consumer group. Policies IAM de MSK costumam restringir por prefixo. */
  groupIdPrefix?: string;
  log?: (message: string) => void;
}

export class MSKService {
  /**
   * Cria uma instância do KafkaJS configurada com a lib aws-msk-iam-sasl-signer-js
   *
   * Sem `roleArn` o token é assinado com as credenciais que a Default Credential
   * Provider Chain já encontra no ambiente — é o caso de quem rodou
   * `aws sts assume-role` e exportou as variáveis, ou de instance/task role: não
   * há segundo AssumeRole e a role atual precisa ter acesso ao cluster.
   */
  public static async createKafkaClient(config: MSKClusterConfig): Promise<Kafka> {
    const roleArn = config.roleArn?.trim();

    return new Kafka({
      clientId: 'vscode-msk-extension',
      brokers: config.brokers,
      ssl: true, // Necessário para a porta 9098
      sasl: {
        mechanism: 'oauthbearer',
        oauthBearerProvider: async () => {
          const authTokenResponse = roleArn
            ? await generateAuthTokenFromRole({
                region: config.region,
                awsRoleArn: roleArn,
                awsRoleSessionName: 'VSCode-MSK-Session'
              })
            : await generateAuthToken({ region: config.region });

          return { value: authTokenResponse.token };
        }
      }
    });
  }

  /**
   * Lista os tópicos disponíveis no cluster.
   */
  public static async listTopics(config: MSKClusterConfig): Promise<string[]> {
    const kafka = await this.createKafkaClient(config);
    const admin = kafka.admin();

    await admin.connect();
    try {
      const topics = await admin.listTopics();
      return topics;
    } finally {
      await admin.disconnect();
    }
  }

  /**
   * Lê os offsets (low/high watermark) de cada partição do tópico.
   */
  public static async fetchTopicOffsets(
    config: MSKClusterConfig,
    topic: string
  ): Promise<PartitionOffsets[]> {
    const kafka = await this.createKafkaClient(config);
    const admin = kafka.admin();

    await admin.connect();
    try {
      const offsets = await admin.fetchTopicOffsets(topic);
      return offsets.map((p) => ({
        partition: p.partition,
        low: Number(p.low),
        high: Number(p.high)
      }));
    } finally {
      await admin.disconnect();
    }
  }

  /**
   * Reúne metadados do tópico: partições, réplicas/ISR, retenção e demais
   * configs, além da contagem de mensagens ainda no log.
   *
   * `describeConfigs` exige kafka-cluster:DescribeTopicDynamicConfiguration; se
   * a role não tiver a permissão, os metadados de partição ainda são devolvidos
   * e as configs voltam vazias em vez de derrubar a consulta inteira.
   */
  public static async describeTopic(
    config: MSKClusterConfig,
    topic: string,
    log: (message: string) => void = () => undefined
  ): Promise<TopicMetadata> {
    const kafka = await this.createKafkaClient(config);
    const admin = kafka.admin();

    await admin.connect();
    try {
      const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
      const topicMetadata = metadata.topics.find((t) => t.name === topic) ?? metadata.topics[0];
      if (!topicMetadata) throw new Error(`Tópico "${topic}" não encontrado no cluster.`);

      const rawOffsets = await admin.fetchTopicOffsets(topic);
      const offsetsByPartition = new Map(
        rawOffsets.map((p) => [p.partition, { low: Number(p.low), high: Number(p.high) }])
      );

      let entries: Record<string, string> = {};
      let overrides: Record<string, string> = {};
      try {
        const described = await admin.describeConfigs({
          includeSynonyms: false,
          resources: [{ type: ConfigResourceTypes.TOPIC, name: topic }]
        });

        for (const entry of described.resources[0]?.configEntries ?? []) {
          const value = entry.configValue ?? '';
          entries[entry.configName] = value;
          // configSource 1 = DYNAMIC_TOPIC_CONFIG: valor setado no tópico.
          if (entry.isDefault === false || Number((entry as any).configSource) === 1) {
            overrides[entry.configName] = value;
          }
        }
      } catch (error: any) {
        log(`[${topic}] não foi possível ler as configs: ${error?.message ?? error}`);
      }

      const partitions: TopicPartitionMetadata[] = topicMetadata.partitions
        .map((p) => {
          const offsets = offsetsByPartition.get(p.partitionId) ?? { low: 0, high: 0 };
          return {
            partition: p.partitionId,
            leader: p.leader,
            replicas: p.replicas,
            isr: p.isr,
            low: offsets.low,
            high: offsets.high,
            messages: Math.max(0, offsets.high - offsets.low)
          };
        })
        .sort((a, b) => a.partition - b.partition);

      const relevant: Record<string, string> = {};
      for (const name of RELEVANT_TOPIC_CONFIGS) {
        if (entries[name] !== undefined) relevant[name] = entries[name];
      }

      return {
        cluster: config.name,
        topic,
        partitions: partitions.length,
        replicationFactor: partitions.length
          ? Math.min(...partitions.map((p) => p.replicas.length))
          : 0,
        underReplicatedPartitions: partitions
          .filter((p) => p.isr.length < p.replicas.length)
          .map((p) => p.partition),
        retention: {
          ms: entries['retention.ms'],
          tempo: formatDuration(entries['retention.ms']) || undefined,
          bytes: entries['retention.bytes'],
          tamanho: formatBytes(entries['retention.bytes']) || undefined
        },
        cleanupPolicy: entries['cleanup.policy'],
        minInSyncReplicas: entries['min.insync.replicas'],
        totalMessages: partitions.reduce((total, p) => total + p.messages, 0),
        overrides,
        configsRelevantes: relevant,
        detalhePartições: partitions
      };
    } finally {
      await admin.disconnect();
    }
  }

  /**
   * Apaga todas as mensagens do tópico sem removê-lo, via DeleteRecords até o
   * high watermark de cada partição.
   *
   * DeleteRecords só move o low watermark: consumidores com offset commitado
   * continuam válidos e o tópico segue existindo com as mesmas configs.
   */
  public static async truncateTopic(
    config: MSKClusterConfig,
    topic: string,
    log: (message: string) => void = () => undefined
  ): Promise<TruncateTopicResult> {
    const kafka = await this.createKafkaClient(config);
    const admin = kafka.admin();

    await admin.connect();
    try {
      const offsets = await admin.fetchTopicOffsets(topic);
      const removed = offsets.reduce((total, p) => total + Math.max(0, Number(p.high) - Number(p.low)), 0);

      // Só as partições com dado; DeleteRecords em partição vazia é ruído.
      const targets = offsets
        .filter((p) => Number(p.high) > Number(p.low))
        .map((p) => ({ partition: p.partition, offset: Number(p.high) }));

      if (targets.length === 0) {
        log(`[${topic}] nada a truncar: todas as partições já estão vazias.`);
        return { topic, removed: 0, partitions: [] };
      }

      await admin.deleteTopicRecords({
        topic,
        partitions: targets.map((t) => ({ partition: t.partition, offset: String(t.offset) }))
      });

      log(
        `[${topic}] truncado até ${targets.map((t) => `p${t.partition}@${t.offset}`).join(' ')} ` +
        `(~${removed} mensagem(ns) removida(s))`
      );

      return { topic, removed, partitions: targets };
    } finally {
      await admin.disconnect();
    }
  }

  /**
   * Lê as últimas mensagens já gravadas no tópico.
   *
   * Caminho principal: fetch direto nas partições, sem consumer group. Ler as
   * últimas mensagens não precisa de coordenação de grupo, e policies IAM de MSK
   * costumam liberar leitura do tópico sem liberar kafka-cluster:*Group* — nesse
   * caso o consumer group falha em "Failed to find group coordinator".
   *
   * Se as internas do kafkajs usadas aqui não existirem (versão diferente da
   * 2.x), cai para o consumer group tradicional.
   */
  public static async fetchMessages(
    config: MSKClusterConfig,
    topic: string,
    options: FetchMessagesOptions = {}
  ): Promise<FetchMessagesResult> {
    const log = options.log ?? (() => undefined);
    const kafka = await this.createKafkaClient(config);

    const cluster = this.createLowLevelCluster(kafka);
    if (cluster) {
      return this.fetchViaDirectFetch(config, cluster, topic, options);
    }

    log(`[${topic}] API interna de fetch indisponível nesta versão do kafkajs; usando consumer group.`);
    return this.fetchViaConsumerGroup(config, kafka, topic, options);
  }

  /**
   * Acessa o `createCluster` privado do client kafkajs (Symbol não exportado nos
   * tipos) para conseguir falar direto com os brokers, sem consumer group.
   * Retorna undefined se a interna não existir na versão instalada.
   */
  private static createLowLevelCluster(kafka: Kafka): any | undefined {
    const symbol = Object.getOwnPropertySymbols(kafka).find(
      (s) => s.description === 'private:Kafka:createCluster'
    );
    if (!symbol) return undefined;

    const createCluster = (kafka as any)[symbol];
    if (typeof createCluster !== 'function') return undefined;

    const cluster = createCluster({
      isolationLevel: 1, // READ_COMMITTED: não expõe transações abertas/abortadas.
      allowAutoTopicCreation: false,
      metadataMaxAge: 300000,
      maxInFlightRequests: null,
      instrumentationEmitter: null
    });

    const required = ['connect', 'disconnect', 'addTargetTopic', 'refreshMetadata', 'findLeaderForPartitions', 'findBroker'];
    if (required.some((method) => typeof cluster?.[method] !== 'function')) return undefined;

    return cluster;
  }

  /**
   * Lê as últimas mensagens fazendo Fetch direto no líder de cada partição.
   */
  private static async fetchViaDirectFetch(
    config: MSKClusterConfig,
    cluster: any,
    topic: string,
    options: FetchMessagesOptions
  ): Promise<FetchMessagesResult> {
    const maxMessages = options.maxMessages ?? 10;
    const timeoutMs = options.timeoutMs ?? 30000;
    const log = options.log ?? (() => undefined);
    const deadline = Date.now() + timeoutMs;

    const topicOffsets = await this.fetchTopicOffsets(config, topic);
    const available = topicOffsets.reduce((total, p) => total + Math.max(0, p.high - p.low), 0);
    log(
      `[${topic}] offsets: ${topicOffsets
        .map((p) => `p${p.partition}=${p.low}..${p.high}`)
        .join(' ') || '(nenhuma partição)'} | disponíveis=${available}`
    );

    // Cursor por partição: de onde ler e até onde ir.
    const cursors = new Map(
      topicOffsets
        .filter((p) => p.high > p.low)
        .map((p) => [
          p.partition,
          { partition: p.partition, next: Math.max(p.low, p.high - maxMessages), high: p.high, collected: 0 }
        ])
    );

    if (cursors.size === 0) {
      return { messages: [], offsets: topicOffsets, available, timedOut: false, mode: 'direct' };
    }

    const messages: MSKMessage[] = [];
    let timedOut = false;

    await cluster.connect();
    try {
      await cluster.addTargetTopic(topic);
      await cluster.refreshMetadata();

      // Fetch devolve o que couber em maxBytes; repetimos até alcançar o high
      // watermark de cada partição (ou acabar o tempo).
      let idleRounds = 0;
      while (cursors.size > 0) {
        if (Date.now() >= deadline) {
          timedOut = true;
          log(`[${topic}] timeout após ${timeoutMs}ms com ${messages.length} mensagem(ns)`);
          break;
        }

        const pending = [...cursors.values()];
        const leaders: Record<string, number[]> = cluster.findLeaderForPartitions(
          topic,
          pending.map((c) => c.partition)
        );

        let receivedAny = false;

        for (const [nodeId, partitions] of Object.entries(leaders)) {
          if (Date.now() >= deadline) break;

          const broker = await cluster.findBroker({ nodeId });
          const request = partitions
            .map((partition) => cursors.get(partition))
            .filter((c): c is NonNullable<typeof c> => c !== undefined)
            .map((c) => ({ partition: c.partition, fetchOffset: String(c.next), maxBytes: 1048576 }));

          if (request.length === 0) continue;

          let response: any;
          try {
            response = await broker.fetch({
              maxWaitTime: Math.max(1000, Math.min(5000, deadline - Date.now())),
              minBytes: 1,
              maxBytes: 10485760,
              isolationLevel: 1,
              topics: [{ topic, partitions: request }]
            });
          } catch (error: any) {
            // Offset expirado por retenção entre o ListOffsets e o Fetch, ou
            // partição sem permissão: registra e para nessas partições.
            log(`[${topic}] fetch falhou no broker ${nodeId}: ${error?.message ?? error}`);
            if (error?.name === 'KafkaJSOffsetOutOfRange') {
              for (const { partition } of request) cursors.delete(partition);
              continue;
            }
            throw error;
          }

          for (const topicResponse of response?.responses ?? []) {
            for (const partitionData of topicResponse.partitions ?? []) {
              const cursor = cursors.get(partitionData.partition);
              if (!cursor) continue;

              const records = this.usableRecords(partitionData, cursor.next);
              if (records.length > 0) receivedAny = true;

              for (const record of records) {
                messages.push({
                  partition: cursor.partition,
                  offset: String(record.offset),
                  key: parsePayload(record.key),
                  value: parsePayload(record.value),
                  timestamp: formatTimestamp(record.timestamp),
                  timestampMs: String(record.timestamp)
                });
                cursor.collected += 1;
              }

              // Avança pelo último offset bruto (inclui control records) para não
              // travar em batches que só têm marcador de transação.
              const raw = partitionData.messages ?? [];
              const lastOffset = raw.length > 0 ? Number(raw[raw.length - 1].offset) : undefined;
              cursor.next = lastOffset !== undefined ? lastOffset + 1 : cursor.next;

              if (cursor.next >= cursor.high || cursor.collected >= maxMessages) {
                cursors.delete(cursor.partition);
              }
            }
          }
        }

        // Sem nenhum registro novo em toda a rodada: evita loop infinito quando o
        // que resta são apenas control records ou offsets já compactados.
        idleRounds = receivedAny ? 0 : idleRounds + 1;
        if (idleRounds >= 3) {
          log(`[${topic}] sem novos registros em 3 rodadas; encerrando leitura.`);
          break;
        }
      }
    } finally {
      await cluster.disconnect().catch(() => undefined);
    }

    log(`[${topic}] fetch direto retornou ${messages.length} mensagem(ns)`);

    const collected = messages
      .sort((a, b) => Number(b.timestampMs) - Number(a.timestampMs))
      .slice(0, maxMessages);

    return { messages: collected, offsets: topicOffsets, available, timedOut, mode: 'direct' };
  }

  /**
   * Descarta control records, mensagens de transações abortadas e registros
   * abaixo do offset pedido (fetch pode devolver offsets anteriores em batches
   * comprimidos) — a mesma filtragem que o consumer do kafkajs faz.
   */
  private static usableRecords(partitionData: any, fetchOffset: number): any[] {
    const messages: any[] = partitionData.messages ?? [];
    const withinOffset = messages.filter((m) => Number(m.offset) >= fetchOffset);

    let filtered = withinOffset;
    try {
      // eslint-disable-next-line @typescript-eslint/no-var-requires
      const filterAbortedMessages = require('kafkajs/src/consumer/filterAbortedMessages');
      filtered = filterAbortedMessages({
        messages: withinOffset,
        abortedTransactions: partitionData.abortedTransactions ?? []
      });
    } catch {
      // Sem a interna do kafkajs seguimos só com o filtro de control records.
    }

    return filtered.filter((m) => !m.isControlRecord);
  }

  /**
   * Caminho legado: consome via consumer group descartável.
   *
   * Falhas do consumer (ex.: GROUP_AUTHORIZATION_FAILED por causa do groupId)
   * chegam pelo evento CRASH e não pela promise do `run()` — por isso o CRASH é
   * escutado e propagado, senão a busca terminaria em timeout parecendo tópico vazio.
   */
  private static async fetchViaConsumerGroup(
    config: MSKClusterConfig,
    kafka: Kafka,
    topic: string,
    options: FetchMessagesOptions
  ): Promise<FetchMessagesResult> {
    const maxMessages = options.maxMessages ?? 10;
    const timeoutMs = options.timeoutMs ?? 30000;
    const groupIdPrefix = options.groupIdPrefix?.trim() || 'vscode-msk';
    const log = options.log ?? (() => undefined);

    const topicOffsets = await this.fetchTopicOffsets(config, topic);
    const available = topicOffsets.reduce((total, p) => total + Math.max(0, p.high - p.low), 0);
    log(
      `[${topic}] offsets: ${topicOffsets
        .map((p) => `p${p.partition}=${p.low}..${p.high}`)
        .join(' ') || '(nenhuma partição)'} | disponíveis=${available}`
    );

    const groupId = `${groupIdPrefix}-${Date.now()}`;

    // Ponto de partida por partição, ignorando as que estão vazias.
    const targets = topicOffsets
      .map((p) => ({
        partition: p.partition,
        start: Math.max(p.low, p.high - maxMessages),
        high: p.high
      }))
      .filter((p) => p.high > p.start);

    if (targets.length === 0) {
      return { messages: [], offsets: topicOffsets, available, timedOut: false, mode: 'consumer-group', groupId };
    }

    const consumer: Consumer = kafka.consumer({
      groupId,
      // Grupo descartável: evita esperar o rebalance padrão de 3s.
      rebalanceTimeout: 10000
    });

    const messages: MSKMessage[] = [];
    const pending = new Map(targets.map((t) => [t.partition, t.high]));

    log(`[${topic}] consumindo com groupId="${groupId}" (timeout ${timeoutMs}ms)`);

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    let timedOut = false;

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      let seeked = false;

      const seekToTargets = () => {
        if (seeked) return; // Re-seek após consumir reiniciaria a leitura em loop.
        seeked = true;
        for (const target of targets) {
          consumer.seek({ topic, partition: target.partition, offset: String(target.start) });
        }
        log(`[${topic}] seek para ${targets.map((t) => `p${t.partition}@${t.start}`).join(' ')}`);
      };

      const finish = (error?: unknown) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        // Desconectar de dentro do eachMessage trava o kafkajs; adiamos um tick.
        setImmediate(() => {
          consumer.disconnect().catch(() => undefined);
        });
        if (error) {
          reject(error instanceof Error ? error : new Error(String(error)));
        } else {
          resolve();
        }
      };

      const timeout = setTimeout(() => {
        timedOut = true;
        log(`[${topic}] timeout após ${timeoutMs}ms com ${messages.length} mensagem(ns)`);
        finish();
      }, timeoutMs);

      // Erros do consumer (auth de grupo, tópico sem permissão de leitura, etc.)
      // só aparecem aqui — a promise do run() já resolveu.
      consumer.on(consumer.events.CRASH, ({ payload }) => {
        const error = payload?.error;
        log(`[${topic}] CRASH: ${error?.message ?? 'erro desconhecido'} (restart=${payload?.restart})`);
        if (payload?.restart) return;
        finish(error ?? new Error('O consumer do Kafka falhou sem detalhes.'));
      });

      // Precisa ser registrado antes do run() para não perder o evento.
      consumer.on(consumer.events.GROUP_JOIN, () => seekToTargets());

      consumer
        .run({
          eachMessage: async ({ partition, message }) => {
            messages.push({
              partition,
              offset: message.offset,
              key: parsePayload(message.key),
              value: parsePayload(message.value),
              timestamp: formatTimestamp(message.timestamp),
              timestampMs: String(message.timestamp)
            });

            const high = pending.get(partition);
            if (high !== undefined && Number(message.offset) >= high - 1) {
              pending.delete(partition);
            }

            if (pending.size === 0) {
              finish();
            }
          }
        })
        .then(() => seekToTargets()) // run() resolve depois do grupo estar ativo.
        .catch(finish);
    });

    // Mais recentes primeiro, limitado ao total pedido.
    const collected = messages
      .sort((a, b) => Number(b.timestampMs) - Number(a.timestampMs))
      .slice(0, maxMessages);

    return { messages: collected, offsets: topicOffsets, available, timedOut, mode: 'consumer-group', groupId };
  }
}
