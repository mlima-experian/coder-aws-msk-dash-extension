# AWS MSK Dashboard Extension

Extensão para VS Code/Coder que permite visualizar e explorar recursos do **AWS Managed Streaming for Apache Kafka (MSK)** diretamente no editor, simplificando a gestão de clusters Kafka em ambientes AWS. Inspirada na extensão **vscode-msk-kafka** publicada no Open VSX.

---

## ✨ Funcionalidades

- Adição de clusters AWS MSK
- Visualização de brokers Kafka
- Listagem de tópicos
- Consulta de consumer groups
- Navegação hierárquica dos recursos do cluster
- Suporte a autenticação AWS
- Integração com AWS Profiles
- Suporte a Assume Role
- Atualização sob demanda das informações do cluster

---

## 📋 Pré-requisitos

Antes de utilizar a extensão, certifique-se de possuir:

- VS Code ou ambiente Coder compatível
- AWS CLI instalado e configurado
- Permissões AWS para:
  - Amazon MSK
  - IAM
  - STS (caso utilize Assume Role)

Configure seu perfil AWS:

```bash
aws configure
```

Valide as credenciais:

```bash
aws sts get-caller-identity
```

---

## 🚀 Instalação

### Open VSX

Instale a extensão disponível no Open VSX:

https://open-vsx.org/extension/mlima-exp/vscode-msk-kafka

Ou execute:

```bash
code --install-extension mlima-exp.vscode-msk-kafka
```

### VSIX

Caso possua o arquivo `.vsix`:

```bash
code --install-extension vscode-msk-kafka.vsix
```

---

## ⚙️ Configuração

Abra o arquivo de configurações do VS Code (`settings.json`) e adicione os parâmetros desejados.

### Utilizando AWS Profile

```json
{
  "msk.aws.profile": "default",
  "msk.aws.region": "us-east-1"
}
```

### Utilizando Assume Role

```json
{
  "msk.aws.profile": "default",
  "msk.aws.region": "us-east-1",
  "msk.aws.roleArn": "arn:aws:iam::123456789012:role/MSKReadOnlyRole"
}
```

---

## 🖥️ Como usar a extensão

### 1. Abrir o painel AWS MSK Dashboard

Após instalar a extensão:

1. Abra o VS Code.
2. Localize o painel lateral da extensão.
3. Clique em **AWS MSK Dashboard**.
4. Expanda a árvore de navegação.

---

### 2. Descobrir clusters MSK

No painel da extensão, selecione:

```text
AWS MSK Dashboard
└── Discover Clusters
```

A extensão irá consultar a AWS e listar os clusters disponíveis para a região configurada.

---

### 3. Visualizar detalhes do cluster

Ao selecionar um cluster você poderá visualizar:

- Nome
- ARN
- Status
- Versão Kafka
- Quantidade de brokers
- Informações de conectividade

Exemplo:

```text
Cluster Dev
├── Brokers
├── Topics
└── Consumer Groups
```

---

### 4. Navegar pelos tópicos

Expanda o item:

```text
Topics
```

Será apresentada a lista de tópicos disponíveis no cluster.

Exemplo:

```text
Topics
├── customer-events
├── orders
├── payments
└── notifications
```

---

### 5. Consultar Consumer Groups

Expanda:

```text
Consumer Groups
```

Visualize informações como:

- Nome do grupo
- Estado
- Membros ativos
- Partições atribuídas

---

### 6. Visualizar Brokers

Expanda:

```text
Brokers
```

Para consultar:

- Broker ID
- Hostname
- Porta
- Estado

---

### 7. Atualizar informações

Para atualizar os dados do dashboard:

- Clique com o botão direito no cluster e selecione **Refresh**
- Ou execute pela Command Palette:

```text
AWS MSK: Refresh Dashboard
```

---

## 🔐 Autenticação AWS

A extensão utiliza a cadeia padrão de credenciais AWS, respeitando a seguinte ordem:

1. Variáveis de ambiente
2. AWS Profile
3. Assume Role
4. ECS Task Role
5. EKS Service Account
6. EC2 Instance Profile

Exemplo:

```bash
export AWS_PROFILE=dev
export AWS_REGION=us-east-1
```

---

## 📁 Estrutura do Dashboard

```text
AWS MSK Dashboard
├── Clusters
│   ├── Cluster-Dev
│   │   ├── Topics
│   │   ├── Brokers
│   │   └── Consumer Groups
│   └── Cluster-Prod
│       ├── Topics
│       ├── Brokers
│       └── Consumer Groups
└── Refresh
```

---

## 🛠️ Desenvolvimento

Clone o projeto:

```bash
git clone https://github.com/mlima-experian/coder-aws-msk-dash-extension.git
cd coder-aws-msk-dash-extension
```

Instale as dependências:

```bash
npm install
```

Execute em modo desenvolvimento:

```bash
npm run watch
```

Pressione **F5** para abrir uma nova janela do VS Code com a extensão carregada.

---

## 📦 Build

Gerar pacote da extensão:

```bash
npm run package
```

ou

```bash
vsce package
```

---

## 🤝 Contribuindo

1. Faça um fork do projeto
2. Crie uma branch para sua feature

```bash
git checkout -b feature/minha-feature
```

3. Faça commit das alterações

```bash
git commit -m "Nova funcionalidade"
```

4. Envie para seu fork

```bash
git push origin feature/minha-feature
```

5. Abra um Pull Request

---

## 📄 Licença

Este projeto está licenciado sob a licença **Apache License 2.0**.

---

## 🔗 Referências

- Repositório GitHub: https://github.com/mlima-experian/coder-aws-msk-dash-extension 【2-aa55d5】
- Open VSX Registry: https://open-vsx.org/extension/mlima-exp/vscode-msk-kafka 【3-a3682d】
