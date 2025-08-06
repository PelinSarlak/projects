import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession

# Spark başlat
spark = SparkSession.builder \
    .appName("FinanceData") \
    .master("local[*]") \
    .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", 1000)
tickers = ["AAPL", "GOOG", "MSFT", "AMZN", "META"]
# Veriyi indir
data = yf.download(tickers, start="2025-07-01", end="2025-07-23", interval='1h', timeout=30)
# Çoklu sütun yapısını aç
data.columns.names = ['Attribute', 'Ticker']
df_flat = data.copy()
df_flat.columns = ['{}_{}'.format(attr, ticker) for attr, ticker in df_flat.columns]

# sonra melt ile uzun formata çevir
df_long = df_flat.reset_index().melt(id_vars='Datetime', var_name='Attribute_Ticker', value_name='Value')

# 'Attribute_Ticker' sütununu ayır
df_long[['Attribute', 'Ticker']] = df_long['Attribute_Ticker'].str.split('_', expand=True)

df_long = df_long.drop(columns='Attribute_Ticker')

print(df_long.head(10
))
print(df_long[df_long.isnull().any(axis=1)])
# Pandas'tan Spark'a çevir
spark_df = spark.createDataFrame(df_long)

# İlk satırlara bakalım
spark_df.show(20, truncate=False)
from pyspark.sql.functions import col
spark_df.filter(col("Value").isNull()).show()
spark_df.printSchema()
spark_df = spark_df.filter(col("Attribute").isin("Close", "Volume", "High", "Low", "Open"))
spark_df = spark_df.na.drop(subset=["Value"])
from pyspark.sql import Window

window_spec = Window.partitionBy("Ticker", "Attribute")
from pyspark.sql.functions import mean, stddev

spark_df = spark_df.withColumn("mean_value", mean(col("Value")).over(window_spec))
spark_df = spark_df.withColumn("stddev_value", stddev(col("Value")).over(window_spec))
# Ortalama ve standart sapma sütunlarıyla ilk 20 satırı göster
spark_df.select("Ticker", "Attribute", "Value", "mean_value", "stddev_value").show(20, truncate=False)
from pyspark.sql.functions import when

spark_df = spark_df.withColumn(
    "stddev_value",
    when(col("stddev_value") == 0, None).otherwise(col("stddev_value"))
)
spark_df.select("Ticker", "Attribute", "Value", "mean_value", "stddev_value").show(20, truncate=False)
spark_df = spark_df.withColumn(
    "z_score",
    (col("Value") - col("mean_value")) / col("stddev_value")
)
anomalies = spark_df.filter(
    col("z_score").isNotNull() & ((col("z_score") > 2) | (col("z_score") < -2))
)
anomalies.select("Datetime", "Ticker", "Attribute", "Value", "z_score").show(50, truncate=False)

anomalies.select("Datetime", "Ticker", "Attribute", "Value", "z_score").show(50, truncate=False)
print(anomalies.count())
anomalies.select("Datetime", "Ticker", "Attribute", "Value", "z_score") \
    .toPandas() \
    .to_csv("C:/Users/test/Desktop/anomalies_data.csv", index=False)
df = pd.read_csv("C:/Users/test/Desktop/anomalies_data.csv")
print(df)










