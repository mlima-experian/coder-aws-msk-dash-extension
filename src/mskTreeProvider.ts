import * as vscode from 'vscode';
import { MSKClusterConfig } from './mskService';

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

  getChildren(element?: vscode.TreeItem): Thenable<vscode.TreeItem[]> {
    if (element) {
      return Promise.resolve([]); 
    } else {
      const clusters = this.context.globalState.get<MSKClusterConfig[]>('msk_clusters', []);
      if (clusters.length === 0) {
        const emptyItem = new vscode.TreeItem('Nenhum cluster cadastrado', vscode.TreeItemCollapsibleState.None);
        return Promise.resolve([emptyItem]);
      }

      const items = clusters.map(cluster => {
        const item = new vscode.TreeItem(cluster.name, vscode.TreeItemCollapsibleState.None);
        item.description = cluster.region;
        item.tooltip = `Role: ${cluster.roleArn}\nBrokers: ${cluster.brokers.join(', ')}`;
        item.iconPath = new vscode.ThemeIcon('server');
        // A linha abaixo é FUNDAMENTAL para o botão "trash" aparecer na linha deste item
        item.contextValue = 'msk-cluster';
        return item;
      });

      return Promise.resolve(items);
    }
  }
}