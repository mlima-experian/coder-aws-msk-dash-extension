import * as vscode from 'vscode';
import { MSKService, MSKClusterConfig } from './mskService';
import { MSKTreeProvider } from './mskTreeProvider';

export function activate(context: vscode.ExtensionContext) {

    const mskTreeProvider = new MSKTreeProvider(context);
    vscode.window.registerTreeDataProvider('msk-clusters-view', mskTreeProvider);

    const output = vscode.window.createOutputChannel('AWS MSK');

    /**
     * Busca e exibe as mensagens de um tópico.
     *
     * Tópico vazio de verdade (high == low em todas as partições) é diferente de
     * "não consegui ler": o segundo vira erro com o motivo, e não uma mensagem
     * dizendo que o tópico está vazio.
     */
    const showTopicMessages = async (cluster: MSKClusterConfig, topic: string) => {
        const settings = vscode.workspace.getConfiguration('awsMsk');
        const maxMessages = settings.get<number>('maxMessages', 10);
        const timeoutMs = settings.get<number>('fetchTimeoutMs', 30000);
        const groupIdPrefix = settings.get<string>('consumerGroupPrefix', 'vscode-msk');

        const result = await MSKService.fetchMessages(cluster, topic, {
            maxMessages,
            timeoutMs,
            groupIdPrefix,
            log: (message) => output.appendLine(message)
        });

        if (result.messages.length === 0) {
            if (result.available === 0) {
                vscode.window.showInformationMessage(`O tópico "${topic}" não possui mensagens.`);
                return;
            }

            output.show(true);
            const hint = result.mode === 'consumer-group'
                ? `Verifique se a role tem kafka-cluster:ReadData no tópico e AlterGroup/DescribeGroup para o group id "${result.groupId}" — ajuste "awsMsk.consumerGroupPrefix" se a policy exigir outro prefixo.`
                : 'Verifique se a role tem kafka-cluster:ReadData no tópico.';
            throw new Error(
                `O tópico "${topic}" tem ~${result.available} mensagem(ns) no broker, mas nenhuma foi lida` +
                `${result.timedOut ? ` em ${timeoutMs}ms` : ''}. ${hint} Detalhes no Output "AWS MSK".`
            );
        }

        if (result.timedOut) {
            vscode.window.showWarningMessage(
                `Busca em "${topic}" encerrada por timeout: ${result.messages.length} de ~${result.available} mensagem(ns) lidas.`
            );
        }

        const doc = await vscode.workspace.openTextDocument({
            content: JSON.stringify(result.messages, null, 2),
            language: 'json'
        });
        await vscode.window.showTextDocument(doc, { preview: false });
    };

    const deleteClusterCmd = vscode.commands.registerCommand('aws-msk.deleteCluster', async (node: vscode.TreeItem) => {
        const clusters = context.globalState.get<MSKClusterConfig[]>('msk_clusters', []);

        // Pega o nome do cluster, seja via clique no botão (node.label) ou via Paleta de Comandos (QuickPick)
        let clusterName = typeof node?.label === 'string' ? node.label : undefined;

        if (!clusterName) {
            const selected = await vscode.window.showQuickPick(clusters.map(c => c.name), {
                placeHolder: 'Selecione o cluster que deseja excluir'
            });
            if (!selected) return;
            clusterName = selected;
        }

        const confirm = await vscode.window.showWarningMessage(`Deseja realmente remover o cluster "${clusterName}"?`, 'Sim', 'Não');
        if (confirm === 'Sim') {
            const filteredClusters = clusters.filter(c => c.name !== clusterName);
            await context.globalState.update('msk_clusters', filteredClusters);
            mskTreeProvider.refresh();
            vscode.window.showInformationMessage(`Cluster MSK "${clusterName}" foi removido.`);
        }
    });

    // 1. Comando: Cadastrar Cluster
    const registerClusterCmd = vscode.commands.registerCommand('aws-msk.registerCluster', async () => {
        const name = await vscode.window.showInputBox({ prompt: 'Nome do Cluster (Apelido)', ignoreFocusOut: true });
        const normalizedName = name?.trim();
        if (!normalizedName) return;

        const region = await vscode.window.showInputBox({ prompt: 'Região AWS (ex: sa-east-1)', value: 'sa-east-1', ignoreFocusOut: true });
        const normalizedRegion = region?.trim();
        if (!normalizedRegion) return;

        const roleArn = await vscode.window.showInputBox({ prompt: 'Role ARN para Assume Role (ex: arn:aws:iam::123456789012:role/MSKAccessRole)', ignoreFocusOut: true });
        const normalizedRoleArn = roleArn?.trim();
        if (!normalizedRoleArn) return;

        const brokersInput = await vscode.window.showInputBox({ prompt: 'Brokers (separados por vírgula com porta 9098 SASL/IAM)', ignoreFocusOut: true });
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
        mskTreeProvider.refresh();
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
                await showTopicMessages(cluster, normalizedTopic);
            } catch (err: any) {
                vscode.window.showErrorMessage(`Erro ao consumir do MSK: ${err.message}`);
            }
        });
    });

    const getRecentEventsCmd = vscode.commands.registerCommand('aws-msk.getRecentEvents', async (cluster?: MSKClusterConfig, topicArg?: string) => {
        const clusters = context.globalState.get<MSKClusterConfig[]>('msk_clusters', []);
        if (clusters.length === 0) {
            vscode.window.showWarningMessage('Nenhum cluster MSK cadastrado. Execute "MSK: Cadastrar Cluster" primeiro.');
            return;
        }

        const selectedCluster = cluster ?? await vscode.window.showQuickPick(clusters.map(c => c.name), {
            placeHolder: 'Selecione o cluster MSK'
        }).then((selectedName) => {
            if (!selectedName) return undefined;
            return clusters.find(c => c.name === selectedName);
        });

        if (!selectedCluster) return;

        const selectedTopic = topicArg ?? await vscode.window.showQuickPick(
            (await MSKService.listTopics(selectedCluster)).map(t => t),
            { placeHolder: 'Selecione o tópico' }
        );

        if (!selectedTopic) return;

        await vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: `Buscando eventos recentes de ${selectedTopic}...`,
            cancellable: false
        }, async () => {
            try {
                await showTopicMessages(selectedCluster, selectedTopic);
            } catch (err: any) {
                vscode.window.showErrorMessage(`Erro ao consultar eventos recentes do MSK: ${err.message}`);
            }
        });
    });

    const refreshCmd = vscode.commands.registerCommand('aws-msk.refreshClusters', () => {
        mskTreeProvider.refresh();
    });

    context.subscriptions.push(output, registerClusterCmd, getMessagesCmd, getRecentEventsCmd, refreshCmd, deleteClusterCmd);
}

export function deactivate() { }