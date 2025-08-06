import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from pyspark.sql import SparkSession

# Spark başlat
spark = SparkSession.builder \
    .appName("FinancePrediction") \
    .master("local[*]") \
    .getOrCreate()

tickers = ["AAPL", "GOOG", "MSFT", "AMZN", "META"]
attributes = ["Open", "Close", "High", "Low", "Volume"]

start_date = "2025-07-01"
end_date = "2025-07-24"  # Veri çekme son tarihi
forecast_start = datetime(2025, 7, 24, 16, 30)  # 16:30 başlangıç
forecast_end = datetime(2025, 8, 1, 22, 30)     # 22:30 bitiş

all_forecasts = []

for ticker in tickers:
    print(f"Veri çekiliyor: {ticker}")
    data = yf.download(ticker, start=start_date, end=end_date, interval='1h', progress=False)

    if data.empty:
        print(f"{ticker} için veri yok, atlanıyor.")
        continue

    if 'Datetime' not in data.columns:
        data = data.reset_index()

    data['Datetime'] = pd.to_datetime(data['Datetime'])

    # Borsa açık saatleri: 16:00 - 22:00 arası (New York saati)
    data['Hour'] = data['Datetime'].dt.hour
    data['Day'] = data['Datetime'].dt.day
    data['Weekday'] = data['Datetime'].dt.weekday
    data = data[(data['Hour'] >= 16) & (data['Hour'] <= 22)]

    for attr in attributes:
        if attr not in data.columns:
            print(f"{attr} sütunu {ticker} için yok, atlanıyor.")
            continue

        df_model = data[['Hour', 'Day', 'Weekday', attr]].dropna()
        if df_model.empty:
            print(f"{ticker} - {attr} için yeterli veri yok, atlanıyor.")
            continue

        X = df_model[['Hour', 'Day', 'Weekday']]
        y = df_model[attr]

        # Eğitim - test bölümü
        X_train, X_test, y_train, y_test = train_test_split(X, y, shuffle=False, test_size=0.2)
        if len(X_train) == 0:
            print(f"{ticker} - {attr} için eğitim verisi yok, atlanıyor.")
            continue

        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)

        # Tahmin aralığı: 24 Temmuz 16:00'dan 1 Ağustos 22:00'ye kadar saatlik, borsa açık saatleri
        future_times = pd.date_range(start=forecast_start + timedelta(hours=1), end=forecast_end, freq='1h')
        future_times = future_times[(future_times.hour >= 16) & (future_times.hour <= 22)]

        future_df = pd.DataFrame({'Datetime': future_times})
        future_df['Hour'] = future_df['Datetime'].dt.hour
        future_df['Day'] = future_df['Datetime'].dt.day
        future_df['Weekday'] = future_df['Datetime'].dt.weekday

        preds = model.predict(future_df[['Hour', 'Day', 'Weekday']])
        future_df[f'{ticker}_{attr}_pred'] = preds

        # Sadece Datetime ve tahmin sütununu al
        all_forecasts.append(future_df[['Datetime', f'{ticker}_{attr}_pred']])

# Tahminleri Datetime sütununa göre birleştir
result_df = all_forecasts[0]
for df in all_forecasts[1:]:
    result_df = result_df.merge(df, on='Datetime', how='outer')

# Spark DataFrame oluştur ve göster
spark_df = spark.createDataFrame(result_df)
# spark_df.show(truncate=False, n=1000)
spark_df.select("Datetime", "AAPL_Close_pred", "GOOG_Close_pred").show(500,truncate=False)
