import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import * as vscode from 'vscode';
import { MSKClusterConfig } from './mskService';

/** Diretório na HOME do usuário onde os dados da extensão ficam gravados. */
export const STORAGE_DIR = path.join(os.homedir(), '.vscode-msk-kafka');
export const CLUSTERS_FILE = path.join(STORAGE_DIR, 'clusters.json');
export const TOPICS_DIR = path.join(STORAGE_DIR, 'topics');

/** Chave antiga: clusters ficavam no globalState do VS Code. */
const LEGACY_STATE_KEY = 'msk_clusters';
const MIGRATION_FLAG = 'msk_clusters_migrated_to_home';

/**
 * Nome de arquivo seguro a partir de um texto livre (nome de tópico/cluster).
 *
 * Nomes de tópico Kafka já usam apenas [a-zA-Z0-9._-], mas a extensão também
 * grava o apelido do cluster, que é digitado pelo usuário e pode conter
 * espaços, barras ou acentos.
 */
export function toSafeFileName(value: string): string {
  const safe = value
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '') // acentos separados pelo NFD
    .replace(/[^a-zA-Z0-9._-]+/g, '_')
    .replace(/^[._]+/, '')
    .slice(0, 120);

  return safe || 'sem-nome';
}

function isValidCluster(value: any): value is MSKClusterConfig {
  return (
    !!value &&
    typeof value.name === 'string' &&
    typeof value.region === 'string' &&
    // roleArn é opcional: sem ele o cluster usa as credenciais já presentes no ambiente.
    (value.roleArn === undefined || value.roleArn === null || typeof value.roleArn === 'string') &&
    Array.isArray(value.brokers) &&
    value.brokers.every((b: unknown) => typeof b === 'string')
  );
}

/**
 * Clusters cadastrados, persistidos em JSON na HOME do usuário.
 *
 * O arquivo é editável à mão e sobrevive à reinstalação da extensão — por isso
 * a leitura ignora entradas malformadas em vez de estourar erro.
 */
export class ClusterStore {
  constructor(
    private readonly context: vscode.ExtensionContext,
    private readonly log: (message: string) => void = () => undefined
  ) {}

  get filePath(): string {
    return CLUSTERS_FILE;
  }

  async list(): Promise<MSKClusterConfig[]> {
    let raw: string;
    try {
      raw = await fs.readFile(CLUSTERS_FILE, 'utf8');
    } catch (error: any) {
      if (error?.code !== 'ENOENT') {
        this.log(`Falha ao ler ${CLUSTERS_FILE}: ${error?.message ?? error}`);
      }
      return [];
    }

    try {
      const parsed = JSON.parse(raw);
      const clusters = Array.isArray(parsed) ? parsed : parsed?.clusters;
      if (!Array.isArray(clusters)) {
        this.log(`Conteúdo inesperado em ${CLUSTERS_FILE}: esperava uma lista de clusters.`);
        return [];
      }
      return clusters.filter(isValidCluster);
    } catch (error: any) {
      this.log(`JSON inválido em ${CLUSTERS_FILE}: ${error?.message ?? error}`);
      return [];
    }
  }

  async save(clusters: MSKClusterConfig[]): Promise<void> {
    await fs.mkdir(STORAGE_DIR, { recursive: true });
    await fs.writeFile(CLUSTERS_FILE, `${JSON.stringify(clusters, null, 2)}\n`, 'utf8');
  }

  async add(cluster: MSKClusterConfig): Promise<void> {
    const clusters = await this.list();
    // Mesmo apelido sobrescreve o cadastro anterior, já que é a chave usada na árvore.
    const others = clusters.filter((c) => c.name !== cluster.name);
    await this.save([...others, cluster]);
  }

  async remove(name: string): Promise<void> {
    const clusters = await this.list();
    await this.save(clusters.filter((c) => c.name !== name));
  }

  /**
   * Move os clusters do globalState (versões <= 0.0.4) para o arquivo na HOME.
   *
   * Roda uma única vez; o globalState é limpo depois para não haver duas fontes
   * de verdade.
   */
  async migrateFromGlobalState(): Promise<void> {
    if (this.context.globalState.get<boolean>(MIGRATION_FLAG, false)) return;

    const legacy = this.context.globalState.get<MSKClusterConfig[]>(LEGACY_STATE_KEY, []);
    const valid = Array.isArray(legacy) ? legacy.filter(isValidCluster) : [];

    if (valid.length > 0) {
      const current = await this.list();
      const merged = [...current];
      for (const cluster of valid) {
        if (!merged.some((c) => c.name === cluster.name)) merged.push(cluster);
      }
      await this.save(merged);
      this.log(`Migrados ${valid.length} cluster(s) do globalState para ${CLUSTERS_FILE}.`);
    }

    await this.context.globalState.update(LEGACY_STATE_KEY, undefined);
    await this.context.globalState.update(MIGRATION_FLAG, true);
  }
}

/**
 * Grava as mensagens do tópico em um arquivo JSON na HOME e abre no editor.
 *
 * Arquivo em vez de documento sem título para a aba levar o nome do tópico e
 * para o conteúdo continuar disponível depois de fechar o editor. O caminho é
 * fixo por cluster/tópico: uma nova busca reaproveita a mesma aba.
 */
export async function openTopicMessagesFile(
  clusterName: string,
  topic: string,
  content: string
): Promise<vscode.TextDocument> {
  return openTopicFile(clusterName, `${toSafeFileName(topic)}.json`, content);
}

/**
 * Mesmo esquema do arquivo de mensagens, para os metadados do tópico. Sufixo
 * próprio para os dois documentos poderem ficar abertos lado a lado.
 */
export async function openTopicMetadataFile(
  clusterName: string,
  topic: string,
  content: string
): Promise<vscode.TextDocument> {
  return openTopicFile(clusterName, `${toSafeFileName(topic)}.metadata.json`, content);
}

async function openTopicFile(
  clusterName: string,
  fileName: string,
  content: string
): Promise<vscode.TextDocument> {
  const dir = path.join(TOPICS_DIR, toSafeFileName(clusterName));
  const filePath = path.join(dir, fileName);
  const uri = vscode.Uri.file(filePath);

  await fs.mkdir(dir, { recursive: true });
  await fs.writeFile(filePath, content, 'utf8');

  // Se o arquivo já estiver aberto, o editor pode continuar mostrando o
  // conteúdo antigo (ou marcar a aba como suja): atualiza o buffer na mão.
  const opened = vscode.workspace.textDocuments.find((d) => d.uri.fsPath === uri.fsPath);
  if (opened && !opened.isClosed && opened.getText() !== content) {
    const edit = new vscode.WorkspaceEdit();
    edit.replace(uri, new vscode.Range(opened.positionAt(0), opened.positionAt(opened.getText().length)), content);
    await vscode.workspace.applyEdit(edit);
    await opened.save();
  }

  const doc = await vscode.workspace.openTextDocument(uri);
  await vscode.window.showTextDocument(doc, { preview: false });
  return doc;
}
