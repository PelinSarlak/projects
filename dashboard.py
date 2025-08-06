import streamlit as st
import pandas as pd
import plotly.express as px

# --- SAYFA YAPISI AYARLARI ---
st.set_page_config(
    page_title="Finansal Anomali Dashboard",
    layout="wide",
    page_icon="📈"
)

# --- ÖZEL DARK MODE STİLİ ---
dark_mode_style = """
<style>
body {
    background-color: #0E1117;
    color: white;
}
.stApp {
    background-color: #0E1117;
    color: white;
}
h1, h2, h3, h4 {
    color: #D1D5DB;
}
.stSelectbox label, .stDateInput label, .stSlider label {
    color: #9CA3AF;
}
</style>
"""
st.markdown(dark_mode_style, unsafe_allow_html=True)

# --- BAŞLIK ---
st.markdown("<h1 style='text-align: center; color: #66d9ef;'>📊 Finansal Anomali Dashboard</h1>", unsafe_allow_html=True)
st.markdown("<p style='text-align: center; color: #AAB2BF;'>Yüklediğiniz finansal verilerdeki olası anomalileri görselleştirin</p>", unsafe_allow_html=True)

# --- CSV YÜKLEME ---
uploaded_file = st.file_uploader("📂 CSV formatında verinizi yükleyin", type="csv", key="file_uploader_1")

if uploaded_file:
    df = pd.read_csv(uploaded_file)

    # Datetime dönüşümü
    df['Datetime'] = pd.to_datetime(df['Datetime'])

    st.markdown("### 🧾 Veri Setiniz (İlk 20 Satır)")
    st.dataframe(df.head(20), use_container_width=True)

    # Tarih aralığı ayarları
    min_date = df['Datetime'].min().date()
    max_date = df['Datetime'].max().date()

    # Tarih filtresi
    date_range = st.date_input(
        "📅 Tarih aralığını seçin",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Tarih filtresine göre veriyi kırp
    if len(date_range) == 2:
        start_date, end_date = date_range
        df = df[(df['Datetime'].dt.date >= start_date) & (df['Datetime'].dt.date <= end_date)]

    st.markdown("### 🔧 Veri Filtreleme ve Ayarlar")
    columns = df.columns.tolist()

    col1, col2, col3 = st.columns(3)
    with col1:
        x_axis = st.selectbox("🧭 X Ekseni", columns, index=columns.index('Datetime') if 'Datetime' in columns else 0)
    with col2:
        y_axis = st.selectbox("📈 Y Ekseni", columns)
    with col3:
        anomaly_threshold = st.slider(
            "🚨 Anomali Z-Score Eşik Değeri",
            min_value=2.0,
            max_value=5.0,
            value=2.0,
            step=0.1
        )

    # Anomali sütunu ekle
    if y_axis == "z_score":
        df['Anomaly'] = df['z_score'].abs() > anomaly_threshold
    else:
        df['Anomaly'] = False

    # GRAFİK ÇİZİMİ
    if st.button("📊 Grafiği Çiz"):
        if y_axis == "z_score":
            fig = px.scatter(
                df,
                x=x_axis,
                y=y_axis,
                color='Anomaly',
                color_discrete_map={True: '#EF476F', False: '#118AB2'},
                template="plotly_dark",
                title=f"{y_axis} vs {x_axis} (Anomali Kırmızı)",
                hover_data=df.columns.tolist()
            )
        else:
            fig = px.line(
                df,
                x=x_axis,
                y=y_axis,
                markers=True,
                template="plotly_dark",
                title=f"{y_axis} vs {x_axis}"
            )

        fig.update_layout(
            title_font_size=22,
            title_x=0.5,
            font=dict(color='white'),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig, use_container_width=True)







    



