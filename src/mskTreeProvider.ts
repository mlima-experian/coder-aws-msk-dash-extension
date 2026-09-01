import * as vscode from 'vscode';
import { MSKClusterConfig, MSKService } from './mskService';

type ClusterTreeItem = vscode.TreeItem & {
  cluster?: MSKClusterConfig;
  topic?: string;
};

export class MSKTreeProvider implements vscode.TreeDataProvider<vscode.TreeItem> {
  private _onDidChangeTreeData: vscode.EventEmitter<vscode.TreeItem | undefined | void> = new vscode.EventEmitter<vscode.TreeItem | undefined | void>();
  readonly onDidChangeTreeData: vscode.Event<vscode.TreeItem | undefined | void> = this._onDidChangeTreeData.event;

  constructor(private context: vscode.ExtensionContext) {}

  refresh(): void {
    this._onDidChangeTreeData.fire();
  }

  getTreeItem(element: vscode.TreeItem): vscode.TreeItem {
    return element;
  }

  async getChildren(element?: vscode.TreeItem): Promise<vscode.TreeItem[]> {
    const clusters = this.context.globalState.get<MSKClusterConfig[]>('msk_clusters', []);

    if (!element) {
      if (clusters.length === 0) {
        const emptyItem = new vscode.TreeItem('Nenhum cluster cadastrado', vscode.TreeItemCollapsibleState.None);
        return [emptyItem];
      }

      return clusters.map((cluster) => {
        const item = new vscode.TreeItem(cluster.name, vscode.TreeItemCollapsibleState.Collapsed) as ClusterTreeItem;
        item.description = cluster.region;
        item.tooltip = `Role: ${cluster.roleArn}\nBrokers: ${cluster.brokers.join(', ')}`;
        item.iconPath = new vscode.ThemeIcon('server');
        item.contextValue = 'msk-cluster';
        item.cluster = cluster;
        return item;
      });
    }

    const currentItem = element as ClusterTreeItem;
    const cluster = currentItem.cluster;
    if (cluster) {
      try {
        const topics = await MSKService.listTopics(cluster);

        if (!topics.length) {
          const emptyItem = new vscode.TreeItem('Nenhum tópico encontrado', vscode.TreeItemCollapsibleState.None);
          emptyItem.contextValue = 'msk-topic-empty';
          return [emptyItem];
        }

        return topics.map((topic) => {
          const item = new vscode.TreeItem(topic, vscode.TreeItemCollapsibleState.None) as ClusterTreeItem;
          item.description = 'tópico';
          item.iconPath = new vscode.ThemeIcon('symbol-namespace');
          item.contextValue = 'msk-topic';
          item.cluster = cluster;
          item.topic = topic;
          item.command = {
            command: 'aws-msk.getRecentEvents',
            title: 'Obter eventos recentes',
            arguments: [cluster, topic]
          };
          return item;
        });
      } catch (error: any) {
        const errorItem = new vscode.TreeItem(`Erro ao listar tópicos: ${error?.message ?? 'desconhecido'}`);
        errorItem.contextValue = 'msk-topic-error';
        return [errorItem];
      }
    }

    return [];
  }
}