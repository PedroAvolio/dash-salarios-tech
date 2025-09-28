import re
import pandas as pd
import plotly.express as px
import streamlit as st

# ── Config da página ────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Dashboard de Salários na Área de Dados",
    page_icon="📊",
    layout="wide",
)

# ── Carregamento com cache ─────────────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def load_data(url: str) -> pd.DataFrame:
    df_ = pd.read_csv(url)
    df_["ano"] = pd.to_numeric(df_["ano"], errors="coerce")
    df_["usd"] = pd.to_numeric(df_["usd"], errors="coerce")
    for c in ["cargo", "senioridade", "contrato", "tamanho_empresa", "remoto"]:
        if c in df_:
            df_[c] = df_[c].astype(str).str.lower()
    return df_.dropna(subset=["usd"])

URL = "https://raw.githubusercontent.com/vqrca/dashboard_salarios_dados/main/dados-imersao-final.csv"
df = load_data(URL)

# ── Barra lateral (filtros) ────────────────────────────────────────────────────
st.sidebar.header("🔍 Filtros")

# Moeda
moeda = st.sidebar.radio("Moeda", ["USD", "BRL"], horizontal=True)
taxa_brl = st.sidebar.number_input("Taxa USD → BRL", value=5.50, step=0.1)
fator = 1.0 if moeda == "USD" else float(taxa_brl)

# Filtros básicos
anos_sel = st.sidebar.multiselect("Ano", sorted(df["ano"].unique()), default=sorted(df["ano"].unique()))
senior_sel = st.sidebar.multiselect("Senioridade", sorted(df["senioridade"].unique()), default=sorted(df["senioridade"].unique()))
contratos_sel = st.sidebar.multiselect("Contrato", sorted(df["contrato"].unique()), default=sorted(df["contrato"].unique()))
tamanhos_sel = st.sidebar.multiselect("Tamanho da Empresa", sorted(df["tamanho_empresa"].unique()), default=sorted(df["tamanho_empresa"].unique()))

# Filtro cargo
cargo_query = st.sidebar.text_input("Cargo (use ; para vários)", placeholder="ex.: data scientist; engineer")
tokens = [t.strip() for t in cargo_query.split(";") if t.strip()]
if tokens:
    pattern = "|".join([re.escape(t) for t in tokens])
    mask_cargo = df["cargo"].str.contains(pattern, case=False, na=False)
else:
    mask_cargo = True

# Faixa salarial
faixa = st.sidebar.slider("Faixa salarial (USD)", int(df["usd"].min()), int(df["usd"].max()), (int(df["usd"].min()), int(df["usd"].max())), step=5000)

# Aplicar filtros
df_filtrado = df[
    (df["ano"].isin(anos_sel)) &
    (df["senioridade"].isin(senior_sel)) &
    (df["contrato"].isin(contratos_sel)) &
    (df["tamanho_empresa"].isin(tamanhos_sel)) &
    mask_cargo &
    (df["usd"].between(faixa[0], faixa[1]))
].copy()

df_filtrado["valor"] = df_filtrado["usd"] * fator
sufixo_moeda = moeda

# ── Título ─────────────────────────────────────────────────────────────────────
st.title("🎲 Dashboard de Análise de Salários")
st.caption("Explore os dados com filtros na barra lateral")

# ── KPIs ──────────────────────────────────────────────────────────────────────
if df_filtrado.empty:
    st.warning("Nenhum dado com os filtros atuais.")
else:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Salário médio", f"{df_filtrado['valor'].mean():,.0f} {sufixo_moeda}")
    c2.metric("Salário máximo", f"{df_filtrado['valor'].max():,.0f} {sufixo_moeda}")
    c3.metric("Registros", f"{len(df_filtrado):,}")
    cargo_freq = df_filtrado["cargo"].mode().iat[0] if not df_filtrado.empty else "—"
    c4.metric("Cargo mais frequente", cargo_freq)

    st.markdown("---")

    # ── Abas ───────────────────────────────────────────────────────────────────
    aba1, aba2, aba3, aba4 = st.tabs(["📈 Visão Geral", "📊 Distribuições", "🗺️ Trabalho", "🧾 Dados"])

    with aba1:
        col1, col2 = st.columns(2)
        top_cargos = (
            df_filtrado.groupby("cargo", as_index=False)["valor"].mean()
            .nlargest(10, "valor")
            .sort_values("valor", ascending=True)
        )
        fig1 = px.bar(top_cargos, x="valor", y="cargo", orientation="h", title=f"Top 10 cargos ({sufixo_moeda})")
        col1.plotly_chart(fig1, use_container_width=True)

        mediana = df_filtrado.groupby(["ano", "senioridade"], as_index=False)["valor"].median()
        fig2 = px.line(mediana, x="ano", y="valor", color="senioridade", markers=True, title=f"Mediana por ano/senioridade ({sufixo_moeda})")
        col2.plotly_chart(fig2, use_container_width=True)

    with aba2:
        col3, col4 = st.columns(2)
        fig3 = px.histogram(df_filtrado, x="valor", nbins=30, title=f"Distribuição salarial ({sufixo_moeda})")
        col3.plotly_chart(fig3, use_container_width=True)

        fig4 = px.box(df_filtrado, x="senioridade", y="valor", points="outliers", title=f"Boxplot por senioridade ({sufixo_moeda})")
        col4.plotly_chart(fig4, use_container_width=True)

    with aba3:
        remoto_contagem = df_filtrado["remoto"].value_counts().reset_index()
        remoto_contagem.columns = ["tipo_trabalho", "quantidade"]
        fig5 = px.pie(remoto_contagem, names="tipo_trabalho", values="quantidade", hole=0.5, title="Tipos de trabalho")
        col5, _ = st.columns([1,1])
        col5.plotly_chart(fig5, use_container_width=True)

    with aba4:
        st.dataframe(df_filtrado.sort_values("valor", ascending=False), use_container_width=True)
        st.download_button("⬇️ Baixar CSV filtrado", df_filtrado.to_csv(index=False).encode("utf-8"), "salarios_filtrado.csv", "text/csv")

st.caption("Valores em USD ou BRL (conversão pela taxa definida)")
