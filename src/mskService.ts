import { Kafka, Consumer } from 'kafkajs';
import { generateAuthTokenFromRole } from 'aws-msk-iam-sasl-signer-js';

export interface MSKClusterConfig {
  name: string;
  region: string;
  roleArn: string;
  brokers: string[];
}

/** Objeto/array quando o payload é JSON; string crua caso contrário. */
export type MSKPayload = unknown;

export interface MSKMessage {
  partition: number;
  offset: string;
  key: MSKPayload;
  value: MSKPayload;
  timestamp: string;
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
   */
  public static async createKafkaClient(config: MSKClusterConfig): Promise<Kafka> {
    return new Kafka({
      clientId: 'vscode-msk-extension',
      brokers: config.brokers,
      ssl: true, // Necessário para a porta 9098
      sasl: {
        mechanism: 'oauthbearer',
        oauthBearerProvider: async () => {
          // A lib resolve o Assume Role automaticamente via Default Credential Provider Chain
          const authTokenResponse = await generateAuthTokenFromRole({
            region: config.region,
            awsRoleArn: config.roleArn,
            awsRoleSessionName: 'VSCode-MSK-Session'
          });

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
                  timestamp: String(record.timestamp)
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
      .sort((a, b) => Number(b.timestamp) - Number(a.timestamp))
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
              timestamp: message.timestamp
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
      .sort((a, b) => Number(b.timestamp) - Number(a.timestamp))
      .slice(0, maxMessages);

    return { messages: collected, offsets: topicOffsets, available, timedOut, mode: 'consumer-group', groupId };
  }
}
