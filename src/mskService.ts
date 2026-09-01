import { Kafka, Consumer } from 'kafkajs';
import { generateAuthTokenFromRole } from 'aws-msk-iam-sasl-signer-js';

export interface MSKClusterConfig {
  name: string;
  region: string;
  roleArn: string;
  brokers: string[];
}

export interface MSKMessage {
  partition: number;
  offset: string;
  key: string;
  value: string;
  timestamp: string;
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
   * Consome as últimas mensagens já gravadas no tópico.
   *
   * Em vez de esperar por mensagens novas (o que devolveria vazio em tópicos sem
   * tráfego no momento), calcula o high watermark de cada partição e faz seek
   * para `high - maxMessages`, lendo o que já está no log.
   */
  public static async fetchMessages(
    config: MSKClusterConfig,
    topic: string,
    maxMessages: number = 10,
    timeoutMs: number = 15000
  ): Promise<MSKMessage[]> {
    const kafka = await this.createKafkaClient(config);
    const admin = kafka.admin();

    await admin.connect();
    let topicOffsets;
    try {
      topicOffsets = await admin.fetchTopicOffsets(topic);
    } finally {
      await admin.disconnect();
    }

    // Ponto de partida por partição, ignorando as que estão vazias.
    const targets = topicOffsets
      .map((p) => {
        const high = Number(p.high);
        const low = Number(p.low);
        return {
          partition: p.partition,
          start: Math.max(low, high - maxMessages),
          high
        };
      })
      .filter((p) => p.high > p.start);

    if (targets.length === 0) {
      return [];
    }

    const consumer: Consumer = kafka.consumer({
      groupId: `vscode-group-${Date.now()}`,
      // Grupo descartável: evita esperar o rebalance padrão de 3s.
      rebalanceTimeout: 10000
    });

    const messages: MSKMessage[] = [];
    const pending = new Map(targets.map((t) => [t.partition, t.high]));

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });

    const collected = await new Promise<MSKMessage[]>((resolve, reject) => {
      let settled = false;

      const finish = (error?: unknown) => {
        if (settled) return;
        settled = true;
        clearTimeout(timeout);
        // Desconectar de dentro do eachMessage trava o kafkajs; adiamos um tick.
        setImmediate(() => {
          consumer.disconnect().catch(() => undefined);
        });
        if (error) {
          reject(error);
        } else {
          resolve(messages);
        }
      };

      const timeout = setTimeout(() => finish(), timeoutMs);

      // Precisa ser registrado antes do run() para não perder o evento.
      consumer.on(consumer.events.GROUP_JOIN, () => {
        for (const target of targets) {
          consumer.seek({ topic, partition: target.partition, offset: String(target.start) });
        }
      });

      consumer
        .run({
          eachMessage: async ({ partition, message }) => {
            messages.push({
              partition,
              offset: message.offset,
              key: message.key ? message.key.toString() : '',
              value: message.value ? message.value.toString() : '',
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
        .catch(finish);
    });

    // Mais recentes primeiro, limitado ao total pedido.
    return collected
      .sort((a, b) => Number(b.timestamp) - Number(a.timestamp))
      .slice(0, maxMessages);
  }
}