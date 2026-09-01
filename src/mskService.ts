import { Kafka, Consumer } from 'kafkajs';
import { generateAuthTokenFromRole } from 'aws-msk-iam-sasl-signer-js';

export interface MSKClusterConfig {
  name: string;
  region: string;
  roleArn: string;
  brokers: string[];
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
   * Consome mensagens recentes de um tópico
   */
  public static async fetchMessages(
    config: MSKClusterConfig,
    topic: string,
    maxMessages: number = 10
  ): Promise<Array<{ offset: string; value: string; timestamp: string }>> {
    const kafka = await this.createKafkaClient(config);
    const consumer: Consumer = kafka.consumer({ groupId: `vscode-group-${Date.now()}` });

    const messages: Array<{ offset: string; key: string; value: string; timestamp: string }> = [];

    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: false });

    return new Promise((resolve, reject) => {
      // 10s de timeout máximo aguardando mensagens
      const timeout = setTimeout(async () => {
        await consumer.disconnect();
        resolve(messages);
      }, 10000);

      consumer.run({
        eachMessage: async ({ message }) => {
          messages.push({
            offset: message.offset,
            key: message.key ? message.key.toString() : '',
            value: message.value ? message.value.toString() : '',
            timestamp: message.timestamp
          });

          if (messages.length >= maxMessages) {
            clearTimeout(timeout);
            await consumer.disconnect();
            resolve(messages);
          }
        }
      }).catch((error) => {
        clearTimeout(timeout);
        reject(error);
      });
    });
  }
}