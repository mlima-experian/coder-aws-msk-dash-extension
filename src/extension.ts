import * as vscode from 'vscode';
import { MSKService, MSKClusterConfig } from './mskService';

export function activate(context: vscode.ExtensionContext) {

  // 1. Comando: Cadastrar Cluster
  const registerClusterCmd = vscode.commands.registerCommand('aws-msk.registerCluster', async () => {
    const name = await vscode.window.showInputBox({ prompt: 'Nome do Cluster (Apelido)' });
    const normalizedName = name?.trim();
    if (!normalizedName) return;

    const region = await vscode.window.showInputBox({ prompt: 'Região AWS (ex: us-east-1)', value: 'sa-east-1' });
    const normalizedRegion = region?.trim();
    if (!normalizedRegion) return;

    const roleArn = await vscode.window.showInputBox({ prompt: 'Role ARN para Assume Role (ex: arn:aws:iam::123456789012:role/MSKAccessRole)' });
    const normalizedRoleArn = roleArn?.trim();
    if (!normalizedRoleArn) return;

    const brokersInput = await vscode.window.showInputBox({ prompt: 'Brokers (separados por vírgula com porta 9098 SASL/IAM)' });
    const brokers = (brokersInput ?? '')
      .split(',')
      .map(b => b.trim())
      .filter(Boolean);

    if (brokers.length === 0) return;

    const config: MSKClusterConfig = {
      name: normalizedName,
      region: normalizedRegion,
      roleArn: normalizedRoleArn,
      brokers
    };

    const clusters = context.globalState.get<MSKClusterConfig[]>('msk_clusters', []);
    clusters.push(config);
    await context.globalState.update('msk_clusters', clusters);

    vscode.window.showInformationMessage(`Cluster MSK "${name}" cadastrado com sucesso!`);
  });

  // 2. Comando: Obter Mensagens
  const getMessagesCmd = vscode.commands.registerCommand('aws-msk.getMessages', async () => {
    const clusters = context.globalState.get<MSKClusterConfig[]>('msk_clusters', []);
    if (clusters.length === 0) {
      vscode.window.showWarningMessage('Nenhum cluster MSK cadastrado. Execute "MSK: Cadastrar Cluster" primeiro.');
      return;
    }

    const selectedClusterName = await vscode.window.showQuickPick(clusters.map(c => c.name), {
      placeHolder: 'Selecione o Cluster MSK'
    });
    if (!selectedClusterName) return;

    const cluster = clusters.find(c => c.name === selectedClusterName);
    if (!cluster) return;

    const topic = await vscode.window.showInputBox({ prompt: 'Nome do Tópico' });
    const normalizedTopic = topic?.trim();
    if (!normalizedTopic) return;

    await vscode.window.withProgress({
      location: vscode.ProgressLocation.Notification,
      title: `Conectando via Assume Role e buscando mensagens de ${topic}...`,
      cancellable: false
    }, async () => {
      try {
        const msgs = await MSKService.fetchMessages(cluster, normalizedTopic, 10);

        // Exibe o resultado em um documento JSON não salvo
        const doc = await vscode.workspace.openTextDocument({
          content: JSON.stringify(msgs, null, 2),
          language: 'json'
        });
        await vscode.window.showTextDocument(doc);
      } catch (err: any) {
        vscode.window.showErrorMessage(`Erro ao consumir do MSK: ${err.message}`);
      }
    });
  });

  context.subscriptions.push(registerClusterCmd, getMessagesCmd);
}

export function deactivate() {}