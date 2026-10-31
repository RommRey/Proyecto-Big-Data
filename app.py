import streamlit as st
import pandas as pd
import numpy as np
import os

# Configuración de página
st.set_page_config(
    page_title="Financial Fraud Analytics | Real-Time Evaluator",
    page_icon="🚨",
    layout="wide"
)

st.title("🚨 Financial Fraud Analytics — Evaluador en Tiempo Real")
st.markdown("Plataforma interactiva para evaluar transacciones individuales y monitorear el pipeline procesado en **Apache Spark**.")

# ---------------------------------------------------------
# BARRA LATERAL: Formulario de Evaluación Manual
# ---------------------------------------------------------
st.sidebar.header("🛠️ Evaluar Nueva Transacción")
st.sidebar.markdown("Ingrese los parámetros de la operación:")

tx_id = st.sidebar.number_input("ID de Transacción", min_value=1000000, value=1000001)
tx_type = st.sidebar.selectbox("Tipo de Transacción", ["TRANSFER", "CASH_OUT", "PAYMENT", "CASH_IN", "DEBIT"])
amount = st.sidebar.number_input("Monto ($ USD)", min_value=1.0, value=12500.0, step=500.0)
old_bal_orig = st.sidebar.number_input("Saldo Origen Antes ($)", min_value=0.0, value=12500.0)
new_bal_orig = st.sidebar.number_input("Saldo Origen Después ($)", min_value=0.0, value=0.0)

btn_eval = st.sidebar.button("🔍 Evaluar Riesgo de Transacción", use_container_width=True)

# Lógica de scoring al instante
if btn_eval:
    balance_error = abs((old_bal_orig - new_bal_orig) - amount)
    
    score = 0.0
    if tx_type in ["TRANSFER", "CASH_OUT"]:
        score += 0.25
    if amount > 5000:
        score += 0.25
    if balance_error > 100:
        score += 0.25
    if old_bal_orig > 0 and new_bal_orig == 0:
        score += 0.25
        
    score = round(min(1.0, score), 2)
    
    st.sidebar.divider()
    st.sidebar.subheader("Resultado de Evaluación:")
    
    if score >= 0.70:
        st.sidebar.error(f"🔴 **HIGH RISK (Score: {score})**")
        st.sidebar.warning("⚠️ Acción: **Bloqueo Preventivo Automático**")
    elif score >= 0.35:
        st.sidebar.warning(f"🟡 **MEDIUM RISK (Score: {score})**")
        st.sidebar.info("📲 Acción: **Requerir Autenticación 2FA / OTP**")
    else:
        st.sidebar.success(f"🟢 **LOW RISK (Score: {score})**")
        st.sidebar.info("✅ Acción: **Aprobación Instantánea**")

# ---------------------------------------------------------
# PANEL PRINCIPAL: Dashboard de Datos Procesados
# ---------------------------------------------------------
csv_path = "data/processed/fraud_detection_dashboard_data.csv"

if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)

    # KPIs Principales
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Transacciones Analizadas", f"{len(df):,}")
    
    high_risk_df = df[df["risk_category"] == "🔴 High Risk"]
    col2.metric("Alertas de Alto Riesgo 🔴", f"{len(high_risk_df):,}")
    col3.metric("Monto Retenido", f"${high_risk_df['amount'].sum():,.2f} USD")
    col4.metric("Tasa de Detección", f"{(len(high_risk_df)/len(df))*100:.1f}%")

    st.divider()

    # Visualizaciones en paralelo
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Distribución por Categoría de Riesgo")
        risk_counts = df["risk_category"].value_counts()
        st.bar_chart(risk_counts)

    with c2:
        st.subheader("Monto de Fraude por Tipo de Operación")
        fraud_by_type = high_risk_df.groupby("type")["amount"].sum()
        st.bar_chart(fraud_by_type)

    st.divider()
    st.subheader("🔍 Monitoreo de Transacciones en Vivo (Muestra de Spark)")
    st.dataframe(
        df[["transaction_id", "step", "type", "amount", "nameOrig", "risk_score", "risk_category"]].head(100),
        use_container_width=True
    )

else:
    st.warning("⚠️ No se encontró el archivo de datos procesados. Por favor ejecuta `python main.py` primero para generar la muestra.")