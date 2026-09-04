import * as vscode from 'vscode';
import { MSKService, MSKClusterConfig, describeAuthMode } from './mskService';
import { MSKTreeProvider, ClusterTreeItem } from './mskTreeProvider';
import { ClusterStore, openTopicMessagesFile, openTopicMetadataFile } from './clusterStore';

export function activate(context: vscode.ExtensionContext) {

    const output = vscode.window.createOutputChannel('AWS MSK');

    // Clusters ficam em JSON na HOME; o globalState das versões antigas é migrado uma vez.
    const store = new ClusterStore(context, (message) => output.appendLine(message));
    const mskTreeProvider = new MSKTreeProvider(store);
    vscode.window.registerTreeDataProvider('msk-clusters-view', mskTreeProvider);

    store.migrateFromGlobalState()
        .then(() => mskTreeProvider.refresh())
        .catch((err: any) => output.appendLine(`Falha ao migrar clusters do globalState: ${err?.message ?? err}`));

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

        const doc = await openTopicMessagesFile(
            cluster.name,
            topic,
            `${JSON.stringify(result.messages, null, 2)}\n`
        );
        output.appendLine(`[${topic}] ${result.messages.length} mensagem(ns) gravada(s) em ${doc.uri.fsPath}`);
    };

    /**
     * Descobre cluster/tópico do comando.
     *
     * Clique no botão inline manda o item da árvore (que já carrega os dois);
     * pela Paleta de Comandos não vem argumento nenhum e é preciso perguntar.
     */
    const resolveTopicTarget = async (
        node?: ClusterTreeItem
    ): Promise<{ cluster: MSKClusterConfig; topic: string } | undefined> => {
        if (node?.cluster && node.topic) return { cluster: node.cluster, topic: node.topic };

        const clusters = await store.list();
        if (clusters.length === 0) {
            vscode.window.showWarningMessage('Nenhum cluster MSK cadastrado. Execute "MSK: Cadastrar Cluster" primeiro.');
            return undefined;
        }

        let cluster = node?.cluster;
        if (!cluster) {
            const selectedName = await vscode.window.showQuickPick(clusters.map(c => c.name), {
                placeHolder: 'Selecione o cluster MSK'
            });
            if (!selectedName) return undefined;
            cluster = clusters.find(c => c.name === selectedName);
            if (!cluster) return undefined;
        }

        const topics = await MSKService.listTopics(cluster);
        const topic = await vscode.window.showQuickPick(topics, { placeHolder: 'Selecione o tópico' });
        if (!topic) return undefined;

        return { cluster, topic };
    };

    // 3. Comando: Metadados do Tópico (botão inline)
    const topicMetadataCmd = vscode.commands.registerCommand('aws-msk.showTopicMetadata', async (node?: ClusterTreeItem) => {
        const target = await resolveTopicTarget(node);
        if (!target) return;

        await vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: `Lendo metadados de ${target.topic}...`,
            cancellable: false
        }, async () => {
            try {
                const metadata = await MSKService.describeTopic(
                    target.cluster,
                    target.topic,
                    (message) => output.appendLine(message)
                );
                await openTopicMetadataFile(
                    target.cluster.name,
                    target.topic,
                    `${JSON.stringify(metadata, null, 2)}\n`
                );
            } catch (err: any) {
                vscode.window.showErrorMessage(`Erro ao ler metadados do tópico: ${err.message}`);
            }
        });
    });

    // 4. Comando: Truncar Tópico (botão inline)
    const truncateTopicCmd = vscode.commands.registerCommand('aws-msk.truncateTopic', async (node?: ClusterTreeItem) => {
        const target = await resolveTopicTarget(node);
        if (!target) return;

        // Apagar mensagens é irreversível: confirmação modal, fora do canto da tela.
        const confirm = await vscode.window.showWarningMessage(
            `Apagar todas as mensagens do tópico "${target.topic}"?`,
            {
                modal: true,
                detail: `Cluster: ${target.cluster.name}\n\nAs mensagens são removidas do log (DeleteRecords até o offset atual) e não podem ser recuperadas. O tópico e suas configurações são mantidos.`
            },
            'Truncar'
        );
        if (confirm !== 'Truncar') return;

        await vscode.window.withProgress({
            location: vscode.ProgressLocation.Notification,
            title: `Truncando ${target.topic}...`,
            cancellable: false
        }, async () => {
            try {
                const result = await MSKService.truncateTopic(
                    target.cluster,
                    target.topic,
                    (message) => output.appendLine(message)
                );

                if (result.removed === 0) {
                    vscode.window.showInformationMessage(`O tópico "${target.topic}" já estava vazio.`);
                    return;
                }

                vscode.window.showInformationMessage(
                    `Tópico "${target.topic}" truncado: ~${result.removed} mensagem(ns) removida(s) em ${result.partitions.length} partição(ões).`
                );
            } catch (err: any) {
                output.show(true);
                vscode.window.showErrorMessage(
                    `Erro ao truncar o tópico: ${err.message}. Verifique se a role tem kafka-cluster:DeleteTopicRecords (ou WriteData) no tópico.`
                );
            }
        });
    });

    const deleteClusterCmd = vscode.commands.registerCommand('aws-msk.deleteCluster', async (node: vscode.TreeItem) => {
        const clusters = await store.list();

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
            await store.remove(clusterName);
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

        // Duas formas de autenticar: assumir uma role específica ou usar o que já
        // está no ambiente (ex.: credenciais exportadas por `aws sts assume-role`).
        const CURRENT_CREDENTIALS = 'Usar a role já assumida (credenciais atuais do ambiente)';
        const ASSUME_ROLE = 'Assumir uma role (informar o ARN)';

        const authMode = await vscode.window.showQuickPick([CURRENT_CREDENTIALS, ASSUME_ROLE], {
            placeHolder: 'Como autenticar no cluster?',
            ignoreFocusOut: true
        });
        if (!authMode) return;

        let normalizedRoleArn: string | undefined;
        if (authMode === ASSUME_ROLE) {
            const roleArn = await vscode.window.showInputBox({ prompt: 'Role ARN para Assume Role (ex: arn:aws:iam::123456789012:role/MSKAccessRole)', ignoreFocusOut: true });
            normalizedRoleArn = roleArn?.trim();
            if (!normalizedRoleArn) return;
        }

        const brokersInput = await vscode.window.showInputBox({ prompt: 'Brokers (separados por vírgula com porta 9098 SASL/IAM)', ignoreFocusOut: true });
        const brokers = (brokersInput ?? '')
            .split(',')
            .map(b => b.trim())
            .filter(Boolean);

        if (brokers.length === 0) return;

        const config: MSKClusterConfig = {
            name: normalizedName,
            region: normalizedRegion,
            // Sem role escolhida o campo fica fora do JSON, e não como string vazia.
            ...(normalizedRoleArn ? { roleArn: normalizedRoleArn } : {}),
            brokers
        };

        await store.add(config);

        vscode.window.showInformationMessage(
            `Cluster MSK "${normalizedName}" cadastrado em ${store.filePath} — ${describeAuthMode(config)}`
        );
        mskTreeProvider.refresh();
    });

    // 2. Comando: Obter Mensagens
    const getMessagesCmd = vscode.commands.registerCommand('aws-msk.getMessages', async () => {
        const clusters = await store.list();
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
            title: `Conectando ao MSK e buscando mensagens de ${topic}...`,
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
        const clusters = await store.list();
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

    context.subscriptions.push(
        output,
        registerClusterCmd,
        getMessagesCmd,
        getRecentEventsCmd,
        refreshCmd,
        deleteClusterCmd,
        topicMetadataCmd,
        truncateTopicCmd
    );
}

export function deactivate() { }