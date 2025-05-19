from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Configuração do Spark
spark = SparkSession.builder.appName("SimulacaoStreaming").getOrCreate()

# Dados simulados
dados = [
{"id_transacao": 1, "status": "concluida", "valor": 100.0,
"timestamp": "2024-12-12T12:00:00Z"},
{"id_transacao": 2, "status": "erro", "valor": 200.0, "timestamp":
"2024-12-12T12:01:00Z"},
{"id_transacao": 3, "status": "concluida", "valor": 300.0,
"timestamp": "2024-12-12T12:02:00Z"}
]

df = spark.createDataFrame(dados)

resultado_contagem = df.groupBy("status").count()
resultado_contagem.show()


# Transformação: Filtrar apenas transações concluídas
contagem_concluidas = resultado_contagem.filter(col("status") == "concluida").select("count").collect()[0][0]
contagem_erros = resultado_contagem.filter(col("status") == "erro").select("count").collect()[0][0]

print(f"Quantidade de transações concluídas: {contagem_concluidas}")
print(f"Quantidade de transações com erro: {contagem_erros}")

# Exportar o resultado da contagem para CSV
output_path = "/home/rcoproc/RCO_PYTHON/resultados_contagem"
resultado_contagem.write.mode("overwrite").option("header", "true").csv(output_path)
print(f"Resultados exportados para: {output_path}")
