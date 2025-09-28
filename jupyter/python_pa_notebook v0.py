# app.py — Sprint 2: ETL + Filtros + Logs de redução de linhas
import io, re, unicodedata
import requests, certifi
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Dashboard de Salários • Sprint 2", page_icon="🎲", layout="wide")

# ---------- Utils ----------
def _norm_str(s):
    if pd.isna(s): return s
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s

def _log_step(logs, name, before, after, note=""):
    logs.append({
        "etapa": name,
        "linhas_antes": int(before),
        "linhas_depois": int(after),
        "reduziu": int(before) - int(after),
        "obs": note
    })

# ---------- ETL ----------
@st.cache_data(ttl=3600, show_spinner=True)
def etl_carregar_tratar(url: str):
    logs = []

    # Extract (com SSL OK)
    r = requests.get(url, timeout=30, verify=certifi.where())
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    _log_step(logs, "Extract (leitura do CSV)", 0, len(df), "Dados brutos carregados da URL")

    # Tipagem básica
    before = len(df)
    if "ano" in df: df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    if "usd" in df: df["usd"] = pd.to_numeric(df["usd"], errors="coerce")
    if "salario" in df: df["salario"] = pd.to_numeric(df["salario"], errors="coerce")
    _log_step(logs, "Tipagem de colunas", before, len(df), "Coerção numérica de ano/usd/salario")

    # Normalização de categorias
    before = len(df)
    for c in ["cargo","senioridade","contrato","tamanho_empresa","remoto","moeda","residencia","empresa"]:
        if c in df:
            df[c] = df[c].map(_norm_str)

    # Mapeamentos
    map_senior = {
        "jr":"junior","jr.":"junior","junior":"junior",
        "pl":"pleno","pleno":"pleno","mid":"pleno","middle":"pleno",
        "sr":"senior","senior":"senior",
        "exec":"executivo","executive":"executivo","executivo":"executivo"
    }
    if "senioridade" in df:
        df["senioridade"] = df["senioridade"].replace(map_senior)

    map_contrato = {"full-time":"integral","fulltime":"integral","part-time":"parcial","parttime":"parcial"}
    if "contrato" in df:
        df["contrato"] = df["contrato"].replace(map_contrato)

    map_tam = {"small":"pequena","medium":"media","mid":"media","large":"grande","big":"grande"}
    if "tamanho_empresa" in df:
        df["tamanho_empresa"] = df["tamanho_empresa"].replace(map_tam)

    _log_step(logs, "Normalização categórica", before, len(df), "senioridade/contrato/tamanho_empresa")

    # Cria coluna base USD (se só existir salario + moeda)
    before = len(df)
    if "usd" not in df.columns:
        if "salario" in df and "moeda" in df:
            df["usd"] = df.apply(lambda r: r["salario"] if r["moeda"]=="usd" else pd.NA, axis=1)
            df["usd"] = pd.to_numeric(df["usd"], errors="coerce")
    _log_step(logs, "Base USD", before, len(df), "Criação da coluna usd se necessário")

    # Drop NA na base de análise
    before = len(df)
    df = df.dropna(subset=["usd"])
    _log_step(logs, "Drop NA (usd)", before, len(df), "Remove linhas sem USD")

    # Filtro de faixa plausível (remove outliers grotescos)
    before = len(df)
    df = df[(df["usd"] >= 3000) & (df["usd"] <= 800000)]
    _log_step(logs, "Faixa salarial plausível", before, len(df), "3000 ≤ usd ≤ 800000")

    # Janela de anos do estudo (2020–2025)
    before = len(df)
    if "ano" in df:
        df = df[df["ano"].between(2020, 2025, inclusive="both")]
    _log_step(logs, "Janela temporal", before, len(df), "Apenas 2020–2025")

    # Deduplicação
    before = len(df)
    subset = [c for c in ["ano","cargo","senioridade","empresa","residencia","usd"] if c in df.columns]
    if subset:
        df = df.drop_duplicates(subset=subset, keep="last")
    _log_step(logs, "Deduplicação", before, len(df), f"Subset: {', '.join(subset)}")

    return df.reset_index(drop=True), logs

# ---------- Sidebar (Filtros) ----------
url_default = "https://raw.githubusercontent.com/PedroAvolio/dash-salarios-tech/refs/heads/main/dados-imersao-final.csv?token=GHSAT0AAAAAADLTZNMJCU67PSX3HYPXRIRK2GZTGCA"
with st.sidebar:
    st.header("Filtros")
    moeda = st.radio("Moeda", ["USD","BRL"], horizontal=True, index=0)
    taxa = st.number_input("Taxa USD → BRL", min_value=0.1, max_value=20.0, step=0.1, value=5.50)

    anos_sel = st.multiselect("Ano", [2020,2021,2022,2023,2024,2025], default=[2020,2021,2022,2023,2024,2025])
    senior_sel = st.multiselect("Senioridade", ["junior","pleno","senior","executivo"], default=["junior","pleno","senior","executivo"])
    contrato_sel = st.multiselect("Tipo de Contrato", ["integral","parcial","freelancer","contrato"], default=["integral","parcial","freelancer","contrato"])
    tam_sel = st.multiselect("Tamanho da Empresa", ["pequena","media","grande"], default=["pequena","media","grande"])
    cargo_query = st.text_input("Cargo (digite partes; use ; para várias)", placeholder="ex.: data scientist; engineer; analytics")
    faixa_usd = st.slider("Faixa salarial (USD base)", 3000, 800000, (3000, 800000), step=1000)
    URL = st.text_input("URL do CSV", value=url_default)

# ---------- Data (ETL) ----------
df, logs = etl_carregar_tratar(URL)

# ---------- Aplicação dos filtros ----------
f = df.copy()
if anos_sel:
    f = f[f["ano"].isin(anos_sel)]
if senior_sel and "senioridade" in f:
    f = f[f["senioridade"].isin(senior_sel)]
if contrato_sel and "contrato" in f:
    f = f[f["contrato"].isin(contrato_sel)]
if tam_sel and "tamanho_empresa" in f:
    f = f[f["tamanho_empresa"].isin(tam_sel)]
if cargo_query.strip() and "cargo" in f:
    parts = [p.strip().lower() for p in cargo_query.split(";") if p.strip()]
    if parts:
        pat = "|".join([re.escape(p) for p in parts])
        f = f[f["cargo"].str.contains(pat, na=False, regex=True)]
f = f[(f["usd"] >= faixa_usd[0]) & (f["usd"] <= faixa_usd[1])]

# Conversão para BRL quando solicitado
val_col = "usd"
if moeda == "BRL":
    f["brl"] = (f["usd"] * taxa).round(2)
    val_col = "brl"

# ---------- KPIs ----------
st.markdown("## 🎲 Dashboard de Análise de Salários na Área de Dados")
colA, colB, colC, colD = st.columns(4)
with colA: st.metric("Salário médio", f"{f[val_col].mean():,.0f} {moeda}")
with colB: st.metric("Salário máximo", f"{f[val_col].max():,.0f} {moeda}")
with colC: st.metric("Total de registros", f"{len(f):,}")
with colD:
    cargo_freq = f["cargo"].mode().iat[0] if "cargo" in f and not f.empty else "—"
    st.metric("Cargo mais frequente", cargo_freq)

# ---------- Charts ----------
left, right = st.columns([1,1])
with left:
    st.subheader("Top 10 cargos por salário médio")
    if not f.empty:
        top = f.groupby("cargo", as_index=False)[val_col].mean().sort_values(val_col, ascending=False).head(10)
        fig = px.bar(top, x=val_col, y="cargo", orientation="h", text=val_col)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Sem dados para os filtros.")

with right:
    st.subheader("Mediana salarial por ano e senioridade")
    if not f.empty and {"ano","senioridade"}.issubset(f.columns):
        med = f.groupby(["ano","senioridade"], as_index=False)[val_col].median()
        fig2 = px.line(med, x="ano", y=val_col, color="senioridade", markers=True)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("Faltam colunas para essa visualização.")

# ---------- Tabela + Download ----------
st.subheader("Prévia dos dados filtrados")
st.dataframe(f.head(50), use_container_width=True)
st.download_button("Baixar CSV filtrado", f.to_csv(index=False).encode("utf-8"), "salarios_filtrado.csv", "text/csv")

# ---------- Logs de ETL (redução de linhas por etapa) ----------
with st.expander("📜 Logs do ETL (Sprint 2) — Redução de linhas por etapa", expanded=True):
    st.dataframe(pd.DataFrame(logs))

    total_bruto = logs[0]["linhas_depois"] if logs else len(df)
    total_final = len(df)
    st.write(f"**Linhas brutas:** {total_bruto:,} → **após ETL:** {total_final:,} "
             f"(**redução total:** {total_bruto - total_final:,})")
