DB Diagnóstico v3.0

O DB Diagnóstico v3.0 é uma solução avançada de monitoramento, auditoria e análise de integridade para ecossistemas de bancos de dados relacionais. Desenvolvida para administradores de banco de dados (DBAs) e engenheiros de software, a ferramenta automatiza a detecção de anomalias de performance e falhas de estrutura em ambientes críticos.

Arquitetura e Tecnologias
A aplicação utiliza uma arquitetura modular baseada em Python, priorizando o isolamento de responsabilidades e a segurança dos dados:

Camada de Interface (UI)
CustomTkinter: Framework para interface gráfica moderna com suporte a temas dinâmicos e alta densidade de informações.

Matplotlib: Motor gráfico integrado para a renderização de dashboards de telemetria e análise de tendências históricas.

Motor de Diagnóstico (Engine)
Multithreading: Execução assíncrona de rotinas de diagnóstico para garantir a responsividade da interface durante operações de I/O intensivas.

Conectividade Nativa: Implementação de drivers especializados para PostgreSQL, MySQL, SQL Server e SQLite, permitindo consultas de baixo nível ao dicionário de dados do sistema.

Segurança e Criptografia
AES-256-GCM: Protocolo de criptografia autenticada para a persistência de credenciais de acesso, garantindo confidencialidade e integridade.

PBKDF2-HMAC-SHA256: Algoritmo de derivação de chave com 480.000 iterações para proteção contra ataques de força bruta à senha mestra.

Funcionalidades Principais
Detecção Inteligente de Conexão
O sistema possui um detector capaz de interpretar automaticamente strings de conexão DSN, arquivos de ambiente (.env), caminhos de arquivos SQLite e formatos ADO.NET.

Auditoria de Saúde do Banco de Dados
Integridade: Identificação de chaves primárias duplicadas e tabelas sem restrições de unicidade.

Performance: Detecção de tabelas sem índices, análise de fragmentação e latência de rede.

Capacidade: Monitoramento de crescimento de arquivos de dados e uso de espaço em disco por tabela.

Monitoramento em Tempo Real
Visualização ativa de processos do servidor, incluindo identificação de consultas lentas (Slow Queries) e detecção de impasses de transação (Locks).

Guia de Operação
1. Configuração e Acesso
Ao iniciar a aplicação, o usuário deve configurar uma senha mestra. Esta senha é utilizada para derivar a chave de criptografia necessária para acessar os perfis de banco de dados salvos.

2. Gestão de Perfis
Utilize o campo de Entrada Inteligente para inserir a string de conexão. O sistema validará o formato e permitirá o salvamento do perfil para execuções futuras.

3. Execução de Diagnósticos
Selecione as categorias de verificação no painel de controle.

Acione o botão de execução para iniciar a varredura.

Os resultados serão exibidos no dashboard e na grade de detalhes, com classificações de status: OK, AVISO, ERRO e INFO.

4. Exportação de Relatórios
O sistema permite a geração de artefatos técnicos para auditoria externa:

Excel (.xlsx): Relatórios tabulares detalhados para análise de dados.

HTML: Relatórios formatados para visualização em navegadores, ideais para compartilhamento executivo.

Instalação
Pré-requisitos
Python 3.10 ou superior.

Drivers ODBC (para conexões SQL Server).

Procedimento
Instale as dependências listadas no arquivo de requisitos:
pip install -r requirements.txt.

Execute o módulo principal:
python app.py.

Para mais informações sobre a implementação dos checks customizados ou integração com Slack/E-mail, consulte os módulos advanced_checks.py e notifications.py no repositório.
