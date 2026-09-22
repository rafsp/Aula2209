# 📊 Data Integration e Pipelines — MBA Engenharia de Dados (FIAP)

> **Disciplina:** Data Integration e Pipelines
> **Professor:** Rafael S Novo Pereira
> **Programa:** MBA Engenharia de Dados — FIAP
> **Carga horária:** 16 horas (4 encontros × 4h)

---

## Sobre a disciplina

Esta disciplina ensina como **mover, transformar e orquestrar dados** de forma confiável em ambientes corporativos. O foco está nos pipelines batch — a espinha dorsal de qualquer operação de dados — cobrindo desde conceitos fundamentais de ETL/ELT até a implementação prática de pipelines com detecção de fraude usando dados reais.

A disciplina está posicionada no currículo entre "Distributed Data Processing & Storage" (que ensina onde os dados moram) e "Stream Processing Pipelines" (que trata de processamento em tempo real). Aqui o aluno aprende a **linha de montagem**: qual peça vai primeiro, quem faz o quê, o que acontece quando algo falha, e como garantir qualidade em cada etapa.

### O que o aluno aprende

Ao final das 16 horas, o aluno será capaz de distinguir abordagens ETL e ELT e justificar quando usar cada uma com base em requisitos de negócio. Terá construído um pipeline funcional de ponta a ponta no Databricks, desde a ingestão de dados brutos até a geração de dashboards interativos com detecção de anomalias e alertas automáticos. Também terá domínio sobre conceitos como DAGs, orquestração, data wrangling, quality gates, backfilling, back-pressure e stale data — temas que diferenciam um pipeline "que funciona" de um pipeline "que funciona em produção".

### Tecnologias utilizadas

O ambiente principal é o **Databricks Free Edition**, uma plataforma serverless e gratuita que roda Apache Spark com notebooks interativos, Delta Lake e Unity Catalog. Os alunos trabalham com Python (PySpark) e SQL diretamente no navegador, sem necessidade de instalar nada localmente. O dataset utilizado nos labs é o **card_transdata.csv** do Kaggle, com aproximadamente 1 milhão de transações reais de cartão de crédito para detecção de fraude.

---

## Estrutura do repositório

```
data-integration-pipelines/
│
├── README.md                          ← Este arquivo
│
├── slides/
│   └── Aula1_Data_Integration.pptx    ← Apresentação da Aula 1 (44 slides, estilo FIAP)
│
├── labs/
│   ├── Lab00_Setup_Databricks.md      ← Passo a passo: conta, volume, upload do dataset
│   ├── Lab01_Pipeline_Fraude.py       ← Notebook Databricks: pipeline Bronze→Silver→Gold
│   └── Lab02_Dashboard.md             ← Feito em aula 
│
├── docs/
│   └── Estrutura_16h_Completa.md      ← Planejamento completo dos 4 encontros (teoria + labs)
│
└── datasets/
    └── README.md                      ← Instruções para baixar o card_transdata.csv do Kaggle
```

---

## Aula 1 — Fundamentos, ETL/ELT, Databricks e Pipeline de Fraude

A primeira aula (4h) cobre os fundamentos teóricos e já coloca o aluno para programar. A estrutura segue uma progressão que vai do conceito ao código.

### Conteúdo teórico (slides)

A apresentação de 44 slides no estilo visual FIAP aborda oito blocos temáticos. Começa com a definição formal de Data Integration (com fontes acadêmicas como Lenzerini 2002 e Reis & Housley 2022), passa pelo impacto financeiro de dados ruins (Gartner estima US$ 12,9 milhões/ano por organização), e apresenta a analogia da fábrica para explicar o que é um pipeline de dados.

Em seguida, entra em ETL com contexto histórico dos anos 90-2000 (quando storage custava US$ 10/GB e licenças Oracle custavam milhões), a analogia do restaurante com cozinha de preparação, e as ferramentas clássicas (Talend, Informatica, SSIS, Pentaho). As limitações do ETL abrem espaço para o ELT, explicado pelas três revoluções da cloud (storage barato, compute elástico, separação storage/compute).

O bloco de Databricks explica Apache Spark do zero (com a analogia dos 100 estagiários), apresenta a plataforma, a arquitetura Medallion (Bronze → Silver → Gold), e a Free Edition que os alunos usam nos labs. Fecha com cenários reais de empresas brasileiras (Nubank, iFood, Magazine Luiza) e perguntas provocativas para discussão em grupo.

### Lab 00 — Configuração do ambiente

O Lab 00 é um guia passo a passo para configurar o Databricks Free Edition usando o e-mail institucional FIAP. Cobre o login via Microsoft, o tour pela plataforma (com os nomes dos menus em português conforme a interface atual), a criação do primeiro notebook com teste de Python e SQL, a criação de um Volume no Unity Catalog, e o upload do dataset card_transdata.csv do Kaggle. Inclui troubleshooting para os problemas mais comuns, como o erro DBFS_DISABLED (que ocorre porque a Free Edition não permite escrita em caminhos /tmp/).

### Lab 01 — Pipeline de detecção de fraude

O notebook (.py, formato nativo do Databricks) implementa um pipeline completo em 10 partes e 40 células. A camada Bronze ingere os dados brutos do CSV e salva como tabela gerenciada no Unity Catalog com metadados de controle. A camada Silver faz o data wrangling: calcula percentis (P95) para definir thresholds de anomalia, cria cinco flags de risco (distância de casa, distância da última transação, valor atípico, sem autenticação forte, compra online sem PIN), calcula um score consolidado de 0 a 5, e classifica cada transação como NORMAL, BAIXO, MEDIO, ALTO ou CRITICO. A camada Gold gera três tabelas de negócio: análise de eficácia do score vs fraude real, dashboard operacional com métricas consolidadas, e lista de alertas para investigação.

O pipeline inclui sete quality checks automáticos (consistência entre camadas, nulos em campos obrigatórios, taxa de fraude no range esperado, distâncias não-negativas, score calculado para todos, quality score geral, e volume mínimo). Os alertas são registrados como tabela Delta no Unity Catalog, com histórico acumulado de execuções. Há também uma célula opcional para envio via webhook externo. Fecha com consultas SQL que mostram insights acionáveis e demonstração de time travel do Delta Lake.

### Lab 02 — Dashboard interativo

O Lab 02 ensina a construir um dashboard nativo do Databricks (AI/BI Dashboard) usando os dados gerados no Lab 01. O guia passo a passo cobre a criação de quatro datasets SQL, montagem de visualizações no canvas (quatro KPIs no topo, gráficos de barras de fraude por nível de risco e por score, tabela de perfil de fraude por combinação de fatores), adição de filtros interativos, publicação, compartilhamento e modo apresentação.

---

## Como usar este material

### Para alunos

Primeiro, siga o **Lab 00** para configurar seu ambiente Databricks e carregar o dataset. Depois, importe o **Lab 01** (arquivo .py) no Databricks clicando em Espaço de trabalho → sua pasta → Importar → arrastar o arquivo. Execute as células na ordem, lendo os comentários e as explicações em Markdown entre cada bloco de código. Ao final, siga o **Lab 02** para criar o dashboard com os dados que o pipeline gerou.

### Para professores

Os slides estão no formato .pptx e podem ser editados no PowerPoint ou Google Slides. O planejamento de 16 horas está detalhado no arquivo `Estrutura_16h_Completa.md`, com minutagem, objetivos de aprendizagem e sugestões de discussão para cada bloco. Os notebooks foram testados no Databricks Free Edition (junho 2025+) e usam exclusivamente tabelas gerenciadas pelo Unity Catalog (sem dependência de DBFS).

---

## Dataset

O dataset utilizado é o **Credit Card Fraud** disponível no Kaggle, publicado por Dhanush Narayanan R. Contém aproximadamente 1 milhão de transações com oito colunas: distância de casa, distância da última transação, razão do valor em relação à mediana do titular, indicadores binários de varejista recorrente, uso de chip, uso de PIN e compra online, além do campo-alvo de fraude.

**Download:** [kaggle.com/datasets/dhanushnarayananr/credit-card-fraud](https://www.kaggle.com/datasets/dhanushnarayananr/credit-card-fraud)

O dataset não está incluído neste repositório por questões de tamanho (~60 MB). Siga as instruções do Lab 00 para fazer o download e upload para o Databricks.

---

## Referências

As fontes utilizadas nos slides e materiais incluem trabalhos acadêmicos e livros técnicos de referência na área.

Reis, J. & Housley, M. — *Fundamentals of Data Engineering*, O'Reilly, 2022. Kimball, R. & Ross, M. — *The Data Warehouse Toolkit*, 3rd Ed., Wiley, 2013. Kleppmann, M. — *Designing Data-Intensive Applications*, O'Reilly, 2017. Densmore, J. — *Data Pipelines Pocket Reference*, O'Reilly, 2021. Lenzerini, M. — *Data Integration: A Theoretical Perspective*, PODS, 2002. Armbrust et al. — *Lakehouse: A New Generation of Open Platforms*, CIDR, 2021.

Documentação oficial: [Apache Spark](https://spark.apache.org) · [Apache Airflow](https://airflow.apache.org) · [Databricks](https://docs.databricks.com) · [Delta Lake](https://delta.io) · [dbt](https://docs.getdbt.com) · [Great Expectations](https://docs.greatexpectations.io)

---

## Licença

Este material é de uso educacional no contexto do MBA Engenharia de Dados da FIAP. A reprodução ou divulgação total ou parcial sem consentimento do autor não é permitida.

---

**Professor:** Rafael S Novo Pereira
**Instituição:** FIAP — Faculdade de Informática e Administração Paulista