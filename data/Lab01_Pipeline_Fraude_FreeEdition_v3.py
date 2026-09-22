# Databricks notebook source

# MAGIC %md
# MAGIC # 🏦 Lab 01 — Pipeline de Detecção de Fraude com Dados Reais
# MAGIC
# MAGIC **Disciplina:** Data Integration e Pipelines
# MAGIC **Professor:** Rafael S Novo Pereira — FIAP MBA Data Engineering
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Contexto de negócio
# MAGIC
# MAGIC Vocês foram contratados como engenheiros de dados de uma fintech brasileira.
# MAGIC A empresa processa milhões de transações de cartão de crédito por mês e precisa
# MAGIC de um **pipeline automatizado** que:
# MAGIC
# MAGIC 1. **Ingira** os dados brutos de transações (camada **Bronze**)
# MAGIC 2. **Limpe e enriqueça** os dados com classificação de risco (camada **Silver**)
# MAGIC 3. **Gere indicadores** de negócio e detecte fraudes (camada **Gold**)
# MAGIC 4. **Valide** a qualidade dos dados a cada etapa
# MAGIC 5. **Dispare alertas** automáticos quando anomalias forem encontradas
# MAGIC
# MAGIC ### Arquitetura do pipeline
# MAGIC
# MAGIC ```
# MAGIC   card_transdata.csv  (Volume do Unity Catalog)
# MAGIC          │
# MAGIC          ▼
# MAGIC   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────────┐
# MAGIC   │   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │────▶│  ALERTA  │
# MAGIC   │  (raw data) │     │  (limpo +   │     │ (métricas + │     │ (tabela  │
# MAGIC   │  tabela UC  │     │  enriquecido)│     │  detecção)  │     │  + SQL)  │
# MAGIC   └─────────────┘     └─────────────┘     └─────────────┘     └──────────┘
# MAGIC ```
# MAGIC
# MAGIC Todas as tabelas são **gerenciadas pelo Unity Catalog** — o padrão da Free Edition.
# MAGIC
# MAGIC ### Dataset: card_transdata.csv (Kaggle)
# MAGIC
# MAGIC | Coluna | O que significa |
# MAGIC |--------|----------------|
# MAGIC | `distance_from_home` | Distância da transação até a casa do titular (km) |
# MAGIC | `distance_from_last_transaction` | Distância da última transação até esta (km) |
# MAGIC | `ratio_to_median_purchase_price` | Valor desta compra ÷ mediana de compras do titular |
# MAGIC | `repeat_retailer` | Loja já usada antes? (0 = não, 1 = sim) |
# MAGIC | `used_chip` | Chip do cartão usado? (0 = não, 1 = sim) |
# MAGIC | `used_pin_number` | Senha PIN digitada? (0 = não, 1 = sim) |
# MAGIC | `online_order` | Compra online? (0 = não, 1 = sim) |
# MAGIC | `fraud` | FRAUDE? (0 = legítima, 1 = fraude) — campo-alvo |
# MAGIC
# MAGIC ### Pré-requisito
# MAGIC - Lab 00 concluído (ambiente e dataset prontos)
# MAGIC - Dataset `card_transdata.csv` carregado no Volume `aulamba`
# MAGIC
# MAGIC **Tempo estimado:** 60 minutos

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 1 — Configuração Inicial

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, when, count, sum as spark_sum, avg,
    max as spark_max, min as spark_min, round as spark_round,
    current_timestamp, percentile_approx, stddev, countDistinct
)
from datetime import datetime

print("=" * 55)
print("  🔧 VERIFICAÇÃO DO AMBIENTE")
print("=" * 55)
print(f"  Spark version:    {spark.version}")
print(f"  Data/hora:        {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"  Ambiente:         Databricks Free Edition")
print(f"  Compute:          Serverless (automático)")
print(f"  Storage:          Unity Catalog (tabelas gerenciadas)")
print("=" * 55)
print("  ✅ Ambiente OK!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## PARTE 2 — Leitura do Dataset
# MAGIC
# MAGIC O arquivo `card_transdata.csv` foi carregado no Volume `aulamba`
# MAGIC no Lab 00. Vamos lê-lo a partir do caminho do Volume.

# COMMAND ----------

# ============================================================
# Caminho do dataset no Volume do Unity Catalog
# Estrutura: /Volumes/{catálogo}/{esquema}/{volume}/{arquivo}
# ============================================================

VOLUME_PATH = "/Volumes/workspace/default/aulamba/card_transdata.csv"

# Ler o CSV
df_raw = spark.read.csv(
    VOLUME_PATH,
    header=True,       # primeira linha é cabeçalho
    inferSchema=True   # Spark detecta os tipos automaticamente
)

total_raw = df_raw.count()
print(f"✅ Dataset carregado com sucesso!")
print(f"📊 Registros: {total_raw:,}")
print(f"📋 Colunas:   {df_raw.columns}")
print(f"📂 Origem:    {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 3 — Camada BRONZE (Dados Brutos)
# MAGIC
# MAGIC A camada Bronze armazena os dados **exatamente como vieram da fonte**,
# MAGIC sem nenhuma transformação. Apenas adicionamos colunas de controle:
# MAGIC quando foi ingerido, de onde veio, qual versão do pipeline.
# MAGIC
# MAGIC ### Por que manter os dados brutos?
# MAGIC
# MAGIC Pensem assim: se vocês recebem um documento original de um cliente,
# MAGIC vocês guardam o original no cofre e trabalham com uma cópia. Se a cópia
# MAGIC ficar suja ou alguém cometer um erro, voltam ao original.
# MAGIC
# MAGIC A Bronze é o "cofre" dos dados originais.
# MAGIC Isso é o **coração do conceito ELT**: carregar primeiro (Load), transformar depois.
# MAGIC
# MAGIC ### Nota técnica: Unity Catalog
# MAGIC
# MAGIC Na Free Edition, usamos **tabelas gerenciadas** pelo Unity Catalog em vez
# MAGIC de caminhos DBFS (que estão desabilitados). Isso é na verdade a prática
# MAGIC recomendada pelo Databricks em ambientes de produção — então vocês
# MAGIC já estão aprendendo do jeito certo.

# COMMAND ----------

# ============================================================
# BRONZE: Salvar dados brutos com metadados de controle
# ============================================================

# Adicionamos 3 colunas de controle (best practice em pipelines!)
df_bronze = df_raw \
    .withColumn("_ingestao_timestamp", current_timestamp()) \
    .withColumn("_fonte", lit("kaggle/card_transdata.csv")) \
    .withColumn("_pipeline_version", lit("1.0.0"))

# Salvar como tabela gerenciada no Unity Catalog
# O formato padrão já é Delta Lake — não precisa especificar
df_bronze.write \
    .mode("overwrite") \
    .saveAsTable("workspace.default.bronze_transacoes")

total_bronze = spark.table("workspace.default.bronze_transacoes").count()
print(f"✅ Camada BRONZE criada!")
print(f"   Registros:  {total_bronze:,}")
print(f"   Formato:    Delta Lake (gerenciada pelo Unity Catalog)")
print(f"   Tabela:     workspace.default.bronze_transacoes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 Data Profiling: conhecendo os dados antes de limpar

# COMMAND ----------

# Visualizar os primeiros registros
display(spark.table("workspace.default.bronze_transacoes").limit(20))

# COMMAND ----------

# Estatísticas descritivas
display(spark.table("workspace.default.bronze_transacoes").describe())

# COMMAND ----------

# Relatório de completude
df_b = spark.table("workspace.default.bronze_transacoes")
total = df_b.count()

print("=" * 65)
print(f"  📊 RELATÓRIO DE PROFILING — CAMADA BRONZE")
print(f"     Total de registros: {total:,}")
print("=" * 65)

for c in df_b.columns:
    if c.startswith("_"):
        continue
    nulos = df_b.filter(col(c).isNull()).count()
    pct = (nulos / total) * 100
    icon = "🔴" if pct > 5 else "🟡" if pct > 0 else "🟢"
    print(f"  {icon} {c:40s} | Nulos: {nulos:>8,} ({pct:5.1f}%)")

print("=" * 65)

# COMMAND ----------

# Distribuição da variável-alvo: fraude vs legítima
print("📊 DISTRIBUIÇÃO DE FRAUDE NO DATASET:")
display(
    df_b.groupBy("fraud")
    .agg(
        count("*").alias("quantidade"),
        spark_round(count("*") / total * 100, 2).alias("percentual")
    )
    .orderBy("fraud")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ❓ Perguntas para a turma (3 minutos)
# MAGIC
# MAGIC 1. O dataset é **balanceado** ou **desbalanceado**? O que isso significa na prática?
# MAGIC 2. Olhando as estatísticas, qual coluna tem os valores mais extremos?
# MAGIC 3. Se vocês fossem o diretor de risco, quais métricas olhariam primeiro toda manhã?

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 4 — Camada SILVER (Dados Limpos + Enriquecidos)
# MAGIC
# MAGIC Na Silver, fazemos o **Data Wrangling**: limpeza e enriquecimento
# MAGIC que transforma dados brutos em dados confiáveis.
# MAGIC
# MAGIC ### Analogia: a linha de inspeção da fábrica
# MAGIC
# MAGIC O produto chega cru (Bronze), passa pela linha de inspeção onde
# MAGIC é lavado, classificado por tamanho e qualidade, recebe selo de
# MAGIC origem, e só depois vai para a prateleira (Gold).
# MAGIC A Silver É essa linha de inspeção.
# MAGIC
# MAGIC Vamos executar 5 passos:

# COMMAND ----------

# ============================================================
# PASSO 1 de 5: Garantir tipos corretos
# ============================================================

df_silver = spark.table("workspace.default.bronze_transacoes")

df_silver = df_silver \
    .withColumn("distance_from_home", col("distance_from_home").cast("double")) \
    .withColumn("distance_from_last_transaction", col("distance_from_last_transaction").cast("double")) \
    .withColumn("ratio_to_median_purchase_price", col("ratio_to_median_purchase_price").cast("double")) \
    .withColumn("repeat_retailer", col("repeat_retailer").cast("integer")) \
    .withColumn("used_chip", col("used_chip").cast("integer")) \
    .withColumn("used_pin_number", col("used_pin_number").cast("integer")) \
    .withColumn("online_order", col("online_order").cast("integer")) \
    .withColumn("fraud", col("fraud").cast("integer"))

print("✅ Passo 1/5 concluído: Tipos de dados verificados")

# COMMAND ----------

# ============================================================
# PASSO 2 de 5: Calcular thresholds estatísticos
# ============================================================
# Usamos o percentil 95 para definir o que é "anormal".
# Se uma transação está nos 5% mais extremos, merece atenção.

stats = df_silver.agg(
    percentile_approx("distance_from_home", 0.95).alias("p95_dist_home"),
    percentile_approx("distance_from_last_transaction", 0.95).alias("p95_dist_last"),
    percentile_approx("ratio_to_median_purchase_price", 0.95).alias("p95_ratio"),
    avg("distance_from_home").alias("media_dist_home"),
    avg("ratio_to_median_purchase_price").alias("media_ratio"),
).collect()[0]

p95_home = stats["p95_dist_home"]
p95_last = stats["p95_dist_last"]
p95_ratio = stats["p95_ratio"]

print("✅ Passo 2/5 concluído: Thresholds calculados")
print()
print("   Percentil 95 (acima disso = anormal):")
print(f"     Distância de casa:             {p95_home:>10.2f} km")
print(f"     Distância da última transação: {p95_last:>10.2f} km")
print(f"     Ratio valor/mediana:           {p95_ratio:>10.2f}x")

# COMMAND ----------

# ============================================================
# PASSO 3 de 5: Criar 5 flags de risco
# ============================================================
# Cada flag representa um fator de suspeita de fraude.

# Flag 1: Transação muito longe de casa
df_silver = df_silver.withColumn("flag_longe_de_casa",
    when(col("distance_from_home") > p95_home, 1).otherwise(0))

# Flag 2: Distância grande desde a última transação
df_silver = df_silver.withColumn("flag_dist_ultima_alta",
    when(col("distance_from_last_transaction") > p95_last, 1).otherwise(0))

# Flag 3: Valor muito acima da mediana do titular
df_silver = df_silver.withColumn("flag_valor_atipico",
    when(col("ratio_to_median_purchase_price") > p95_ratio, 1).otherwise(0))

# Flag 4: Sem chip E sem PIN (nenhuma autenticação forte)
df_silver = df_silver.withColumn("flag_sem_autenticacao",
    when((col("used_chip") == 0) & (col("used_pin_number") == 0), 1).otherwise(0))

# Flag 5: Compra online sem PIN (cenário clássico de vazamento)
df_silver = df_silver.withColumn("flag_online_sem_pin",
    when((col("online_order") == 1) & (col("used_pin_number") == 0), 1).otherwise(0))

print("✅ Passo 3/5 concluído: 5 flags de risco criadas")
print()
for flag in ["flag_longe_de_casa", "flag_dist_ultima_alta", "flag_valor_atipico",
             "flag_sem_autenticacao", "flag_online_sem_pin"]:
    qtd = df_silver.filter(col(flag) == 1).count()
    pct = qtd / df_silver.count() * 100
    print(f"     {flag:30s}  →  {qtd:>8,} transações ({pct:.1f}%)")

# COMMAND ----------

# ============================================================
# PASSO 4 de 5: Score de risco consolidado (0 a 5)
# ============================================================

df_silver = df_silver.withColumn("score_risco",
    col("flag_longe_de_casa") +
    col("flag_dist_ultima_alta") +
    col("flag_valor_atipico") +
    col("flag_sem_autenticacao") +
    col("flag_online_sem_pin")
)

df_silver = df_silver.withColumn("nivel_risco",
    when(col("score_risco") >= 4, "CRITICO")
    .when(col("score_risco") >= 3, "ALTO")
    .when(col("score_risco") >= 2, "MEDIO")
    .when(col("score_risco") >= 1, "BAIXO")
    .otherwise("NORMAL")
)

print("✅ Passo 4/5 concluído: Score de risco calculado")
print()
df_silver.groupBy("nivel_risco").agg(count("*").alias("quantidade")).orderBy("quantidade", ascending=False).show()

# COMMAND ----------

# ============================================================
# PASSO 5 de 5: Flags de qualidade + salvar Silver
# ============================================================

df_silver = df_silver \
    .withColumn("qa_distancia_valida",
        when(col("distance_from_home") >= 0, True).otherwise(False)) \
    .withColumn("qa_ratio_valido",
        when(col("ratio_to_median_purchase_price") >= 0, True).otherwise(False)) \
    .withColumn("qa_registro_ok",
        when(
            (col("qa_distancia_valida") == True) &
            (col("qa_ratio_valido") == True) &
            col("fraud").isNotNull(),
            True
        ).otherwise(False)
    ) \
    .withColumn("_silver_timestamp", current_timestamp())

# Salvar como tabela gerenciada no Unity Catalog
df_silver.write \
    .mode("overwrite") \
    .saveAsTable("workspace.default.silver_transacoes")

ok = df_silver.filter(col("qa_registro_ok") == True).count()
fail = df_silver.filter(col("qa_registro_ok") == False).count()

print(f"✅ Passo 5/5 concluído: Camada SILVER salva!")
print(f"   Registros:     {df_silver.count():,}")
print(f"   QA aprovados:  {ok:,}")
print(f"   QA reprovados: {fail:,}")
print(f"   Tabela:        workspace.default.silver_transacoes")

# COMMAND ----------

display(spark.table("workspace.default.silver_transacoes").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 5 — Camada GOLD (Métricas de Negócio)
# MAGIC
# MAGIC A Gold é a camada que o **negócio consome**. Aqui só tem métricas
# MAGIC prontas para decisão — nada de dado bruto ou intermediário.

# COMMAND ----------

df_s = spark.table("workspace.default.silver_transacoes")

# ============================================================
# GOLD 1: Eficácia do score — nosso modelo detecta fraude?
# ============================================================

gold_eficacia = df_s.groupBy("nivel_risco").agg(
    count("*").alias("total_transacoes"),
    spark_sum("fraud").alias("fraudes_reais"),
    spark_round(avg("fraud") * 100, 2).alias("taxa_fraude_pct"),
    spark_round(avg("distance_from_home"), 1).alias("dist_media_casa"),
    spark_round(avg("ratio_to_median_purchase_price"), 2).alias("ratio_medio"),
).orderBy("taxa_fraude_pct", ascending=False)

gold_eficacia.write.mode("overwrite").saveAsTable("workspace.default.gold_eficacia_risco")

print("✅ Gold Table 1: Análise de eficácia do score")
display(gold_eficacia)

# COMMAND ----------

# ============================================================
# GOLD 2: Dashboard operacional
# ============================================================
total = df_s.count()
total_fraudes = df_s.filter(col("fraud") == 1).count()
total_critico = df_s.filter(col("nivel_risco") == "CRITICO").count()
total_alto = df_s.filter(col("nivel_risco") == "ALTO").count()

fraudes_capturadas = df_s.filter(
    (col("fraud") == 1) & (col("score_risco") >= 3)
).count()
taxa_deteccao = (fraudes_capturadas / total_fraudes * 100) if total_fraudes > 0 else 0

falsos_positivos = df_s.filter(
    (col("fraud") == 0) & (col("score_risco") >= 3)
).count()

print("=" * 60)
print("  📊 DASHBOARD OPERACIONAL — FINTECH LAB 01")
print("=" * 60)
print(f"  📌 Total de transações:          {total:>12,}")
print(f"  🚨 Fraudes reais no dataset:     {total_fraudes:>12,}")
print(f"  📈 Taxa de fraude:               {total_fraudes/total*100:>11.2f}%")
print(f"  🔴 Transações CRÍTICAS:          {total_critico:>12,}")
print(f"  🟠 Transações ALTO risco:        {total_alto:>12,}")
print(f"  ✅ Fraudes capturadas (score≥3): {fraudes_capturadas:>12,}")
print(f"  🎯 Taxa de detecção:             {taxa_deteccao:>11.2f}%")
print(f"  ⚠️  Falsos positivos (score≥3):  {falsos_positivos:>12,}")
print("=" * 60)

# Guardar métricas para os quality checks
metricas = {
    "total_transacoes": total,
    "total_fraudes": total_fraudes,
    "taxa_fraude_pct": round(total_fraudes/total*100, 2),
    "transacoes_criticas": total_critico,
    "fraudes_capturadas": fraudes_capturadas,
    "taxa_deteccao_pct": round(taxa_deteccao, 2),
    "falsos_positivos": falsos_positivos,
}

# COMMAND ----------

# ============================================================
# GOLD 3: Tabela de alertas — transações para investigação
# ============================================================

gold_alertas = df_s \
    .filter(col("score_risco") >= 3) \
    .select(
        "distance_from_home", "distance_from_last_transaction",
        "ratio_to_median_purchase_price", "repeat_retailer",
        "used_chip", "used_pin_number", "online_order",
        "fraud", "score_risco", "nivel_risco",
    ) \
    .orderBy("score_risco", ascending=False)

gold_alertas.write.mode("overwrite").saveAsTable("workspace.default.gold_alertas")

print(f"🚨 Transações para investigação: {gold_alertas.count():,}")
display(gold_alertas.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 6 — Quality Gate (Validações Automáticas)
# MAGIC
# MAGIC Verificações que confirmam que os dados fazem sentido.
# MAGIC Se algo falhar, o pipeline alerta — em vez de propagar dado errado.

# COMMAND ----------

def executar_quality_checks():
    """
    7 verificações de qualidade no pipeline completo.
    """
    df_b = spark.table("workspace.default.bronze_transacoes")
    df_s = spark.table("workspace.default.silver_transacoes")
    total_b = df_b.count()
    total_s = df_s.count()
    resultados = []
    alertas = []

    # CHECK 1: Consistência Bronze → Silver
    diff = abs(total_b - total_s)
    ok = diff == 0
    resultados.append({"check": "Consistência Bronze == Silver", "status": "PASS" if ok else "FAIL", "detalhe": f"Bronze: {total_b:,} | Silver: {total_s:,} | Diff: {diff}"})
    if not ok: alertas.append(f"🔴 Perda de {diff} registros entre Bronze e Silver!")

    # CHECK 2: Campo fraud sem nulos
    nulos_fraud = df_s.filter(col("fraud").isNull()).count()
    ok = nulos_fraud == 0
    resultados.append({"check": "Campo 'fraud' sem nulos", "status": "PASS" if ok else "FAIL", "detalhe": f"{nulos_fraud} nulos"})
    if not ok: alertas.append(f"🔴 Campo fraud tem {nulos_fraud} nulos!")

    # CHECK 3: Taxa de fraude no range esperado
    taxa = df_s.filter(col("fraud") == 1).count() / total_s * 100
    ok = 0.5 <= taxa <= 15
    resultados.append({"check": "Taxa de fraude entre 0.5% e 15%", "status": "PASS" if ok else "WARN", "detalhe": f"{taxa:.2f}%"})
    if not ok: alertas.append(f"🟡 Taxa de fraude fora do range: {taxa:.2f}%")

    # CHECK 4: Distâncias não-negativas
    neg = df_s.filter(col("distance_from_home") < 0).count()
    ok = neg == 0
    resultados.append({"check": "Distâncias não-negativas", "status": "PASS" if ok else "FAIL", "detalhe": f"{neg} negativos"})
    if not ok: alertas.append(f"🔴 {neg} transações com distância negativa!")

    # CHECK 5: Score calculado para todos
    sem = df_s.filter(col("score_risco").isNull()).count()
    ok = sem == 0
    resultados.append({"check": "Score de risco calculado para todos", "status": "PASS" if ok else "FAIL", "detalhe": f"{sem} sem score"})

    # CHECK 6: Quality score > 95%
    qa_ok = df_s.filter(col("qa_registro_ok") == True).count()
    pct_ok = qa_ok / total_s * 100
    ok = pct_ok >= 95
    resultados.append({"check": "Quality score geral > 95%", "status": "PASS" if ok else "WARN", "detalhe": f"{pct_ok:.2f}% aprovados"})
    if not ok: alertas.append(f"🟡 Apenas {pct_ok:.2f}% dos registros passaram no QA")

    # CHECK 7: Volume mínimo
    ok = total_s >= 100000
    resultados.append({"check": "Volume mínimo (> 100K registros)", "status": "PASS" if ok else "FAIL", "detalhe": f"{total_s:,} registros"})
    if not ok: alertas.append(f"🔴 Apenas {total_s:,} registros (esperado > 100K)")

    return resultados, alertas


resultados, alertas = executar_quality_checks()

print("=" * 72)
print("  📋 RELATÓRIO DE QUALIDADE — PIPELINE FINTECH")
print("=" * 72)
for r in resultados:
    ic = "✅" if r["status"] == "PASS" else "⚠️" if r["status"] == "WARN" else "❌"
    print(f"  {ic} [{r['status']:4s}] {r['check']:42s} | {r['detalhe']}")
print("=" * 72)

tp = sum(1 for r in resultados if r["status"] == "PASS")
tw = sum(1 for r in resultados if r["status"] == "WARN")
tf = sum(1 for r in resultados if r["status"] == "FAIL")
print(f"\n  Resultado: {tp} ✅ PASS  |  {tw} ⚠️ WARN  |  {tf} ❌ FAIL")

if alertas:
    print(f"\n  🚨 ALERTAS ({len(alertas)}):")
    for a in alertas: print(f"     {a}")
else:
    print(f"\n  🎉 Nenhum alerta — pipeline saudável!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 7 — Registro de Alertas
# MAGIC
# MAGIC Na Free Edition, o acesso à internet de saída é restrito,
# MAGIC então em vez de enviar para um webhook externo, vamos salvar
# MAGIC o alerta como uma **tabela Delta no Unity Catalog**.
# MAGIC
# MAGIC Na prática empresarial, esse padrão é muito comum: o pipeline
# MAGIC grava alertas numa tabela, e um dashboard ou sistema de
# MAGIC monitoramento consulta essa tabela periodicamente.
# MAGIC
# MAGIC O Databricks inclusive tem **Alertas nativos** (menu SQL → Alertas)
# MAGIC que podem monitorar essa tabela e enviar notificações por e-mail.

# COMMAND ----------

import json as json_lib

alerta_data = [{
    "pipeline_nome": "Lab01_Fraude_Fintech",
    "timestamp_execucao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "total_checks": len(resultados),
    "checks_passed": sum(1 for r in resultados if r["status"] == "PASS"),
    "checks_failed": sum(1 for r in resultados if r["status"] == "FAIL"),
    "checks_warned": sum(1 for r in resultados if r["status"] == "WARN"),
    "total_transacoes": metricas["total_transacoes"],
    "total_fraudes": metricas["total_fraudes"],
    "taxa_deteccao_pct": metricas["taxa_deteccao_pct"],
    "alertas_detalhe": json_lib.dumps(alertas, ensure_ascii=False),
    "resultado": "OK" if not alertas else "ATENCAO",
}]

df_alerta = spark.createDataFrame(alerta_data)

# Usar append para manter histórico de execuções
# Na primeira vez, a tabela é criada. Nas seguintes, acumula.
df_alerta.write.mode("append").saveAsTable("workspace.default.log_alertas_pipeline")

print("✅ Alerta registrado na tabela log_alertas_pipeline!")
print(f"   Tabela: workspace.default.log_alertas_pipeline")
display(df_alerta)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔔 Webhook externo (opcional)
# MAGIC
# MAGIC Se quiser testar o envio para webhook externo (pode funcionar
# MAGIC ou não dependendo das restrições de rede), tente a célula abaixo.
# MAGIC Caso dê erro de conexão, é normal — use a tabela de alertas acima.

# COMMAND ----------

# === OPCIONAL: Webhook externo ===
# Acesse https://webhook.site, copie a URL e cole abaixo.
# Se der erro de conexão, é a restrição da Free Edition.

WEBHOOK_URL = "https://webhook.site/SUA-URL-AQUI"

import urllib.request, urllib.error, json

if "SUA-URL-AQUI" not in WEBHOOK_URL:
    payload = json.dumps({
        "pipeline": "Lab01_Fraude_Fintech",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "quality_checks": resultados,
        "alertas": alertas,
        "metricas": metricas,
    }, ensure_ascii=False).encode("utf-8")

    try:
        req = urllib.request.Request(WEBHOOK_URL, data=payload,
            headers={"Content-Type": "application/json"}, method="POST")
        resp = urllib.request.urlopen(req, timeout=15)
        print(f"✅ Webhook enviado! (HTTP {resp.getcode()})")
    except urllib.error.URLError as e:
        print(f"⚠️ Webhook não disponível (restrição da Free Edition): {e}")
        print("   Use a tabela log_alertas_pipeline como alternativa.")
else:
    print("ℹ️ Webhook não configurado. Use a tabela log_alertas_pipeline.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 8 — Consultas SQL
# MAGIC
# MAGIC O Databricks permite usar SQL diretamente com `%sql`.
# MAGIC As tabelas do Unity Catalog são acessíveis por nome completo.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Eficácia do score: quanto maior o nível, mais fraude?
# MAGIC SELECT * FROM workspace.default.gold_eficacia_risco
# MAGIC ORDER BY taxa_fraude_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Qual combinação de fatores tem MAIOR taxa de fraude?
# MAGIC SELECT
# MAGIC     CASE WHEN online_order = 1 THEN 'Online' ELSE 'Presencial' END AS canal,
# MAGIC     CASE WHEN used_chip = 1 THEN 'Com chip' ELSE 'Sem chip' END AS chip,
# MAGIC     CASE WHEN used_pin_number = 1 THEN 'Com PIN' ELSE 'Sem PIN' END AS pin,
# MAGIC     COUNT(*) AS total,
# MAGIC     SUM(fraud) AS fraudes,
# MAGIC     ROUND(AVG(fraud) * 100, 2) AS taxa_fraude_pct
# MAGIC FROM workspace.default.silver_transacoes
# MAGIC GROUP BY online_order, used_chip, used_pin_number
# MAGIC ORDER BY taxa_fraude_pct DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 20 transações mais suspeitas que confirmaram fraude
# MAGIC SELECT
# MAGIC     score_risco, nivel_risco,
# MAGIC     ROUND(distance_from_home, 1) AS dist_casa_km,
# MAGIC     ROUND(ratio_to_median_purchase_price, 1) AS ratio_valor,
# MAGIC     online_order, used_chip, used_pin_number
# MAGIC FROM workspace.default.gold_alertas
# MAGIC WHERE fraud = 1
# MAGIC ORDER BY score_risco DESC, distance_from_home DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Histórico de execuções do pipeline (log de alertas)
# MAGIC SELECT * FROM workspace.default.log_alertas_pipeline
# MAGIC ORDER BY timestamp_execucao DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 9 — Delta Lake: Time Travel
# MAGIC
# MAGIC O Delta Lake guarda versões dos dados a cada escrita.
# MAGIC Você pode "viajar no tempo" e acessar qualquer versão anterior.

# COMMAND ----------

# Histórico de versões da Silver
display(spark.sql("DESCRIBE HISTORY workspace.default.silver_transacoes"))

# COMMAND ----------

# Ler a versão 0 (primeira escrita)
df_v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .table("workspace.default.silver_transacoes")

print(f"📜 Versão 0 da Silver: {df_v0.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## PARTE 10 — Limpeza (Opcional)
# MAGIC
# MAGIC Se quiser limpar as tabelas criadas durante o lab:

# COMMAND ----------

# Descomente para limpar:

# spark.sql("DROP TABLE IF EXISTS workspace.default.bronze_transacoes")
# spark.sql("DROP TABLE IF EXISTS workspace.default.silver_transacoes")
# spark.sql("DROP TABLE IF EXISTS workspace.default.gold_eficacia_risco")
# spark.sql("DROP TABLE IF EXISTS workspace.default.gold_alertas")
# spark.sql("DROP TABLE IF EXISTS workspace.default.log_alertas_pipeline")
# print("🧹 Tabelas removidas!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📝 Desafios para Entregar
# MAGIC
# MAGIC ### Desafio 1 — Nova regra de risco (obrigatório)
# MAGIC
# MAGIC Crie **pelo menos 1 nova flag de risco** além das 5 que implementamos.
# MAGIC Ideias:
# MAGIC - `ratio_to_median_purchase_price` > 10 (compra 10x acima da mediana)
# MAGIC - Compra online + sem chip + sem PIN + loja nova (4 fatores simultâneos)
# MAGIC - Distância de casa > 200 km E valor acima de 5x a mediana
# MAGIC
# MAGIC Recalcule o `score_risco` e compare: a taxa de detecção melhorou?
# MAGIC
# MAGIC ### Desafio 2 — Novos quality checks (obrigatório)
# MAGIC
# MAGIC Adicione **2 novos checks** à função `executar_quality_checks`:
# MAGIC - Percentual de transações CRÍTICAS não deve passar de X% (defina e justifique)
# MAGIC - Média de `distance_from_home` deve ser positiva
# MAGIC
# MAGIC ### Desafio 3 — Análise SQL (bônus)
# MAGIC
# MAGIC Escreva uma query SQL que responda:
# MAGIC
# MAGIC > **"Qual é o perfil típico de uma transação fraudulenta?"**
# MAGIC
# MAGIC Use: online/presencial, chip/sem chip, PIN/sem PIN, distância,
# MAGIC ratio de valor. Formate para um executivo entender.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Professor:** Rafael S Novo Pereira
# MAGIC **Disciplina:** Data Integration e Pipelines — FIAP MBA Data Engineering
