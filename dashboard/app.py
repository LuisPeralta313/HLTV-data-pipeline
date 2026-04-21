import os
import duckdb
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="HLTV CS2 Dashboard", layout="wide")

_SUPABASE_URL = os.getenv("SUPABASE_URL")

@st.cache_data
def load_table() -> pd.DataFrame:
    if _SUPABASE_URL:
        import psycopg2
        pg = psycopg2.connect(
            host=os.environ["SUPABASE_HOST"],
            port=int(os.environ["SUPABASE_PORT"]),
            user=os.environ["SUPABASE_USER"],
            password=os.environ["SUPABASE_PW"],
            dbname=os.environ["SUPABASE_DB"],
            sslmode="require",
            connect_timeout=15,
        )
        df = pd.read_sql("SELECT * FROM fct_match_performance", pg)
        pg.close()
        return df
    else:
        con = duckdb.connect("warehouse/hltv.duckdb", read_only=True)
        df = con.execute("SELECT * FROM main_marts.fct_match_performance").df()
        con.close()
        return df

df = load_table()

# ---------------------------------------------------------------------------
# 1. RESUMEN GENERAL
# ---------------------------------------------------------------------------
st.title("HLTV CS2 — Pipeline Dashboard")
st.markdown("---")

total_matches   = df["match_id"].nunique()
total_players   = df["player_name"].nunique()
date_min        = df["match_date"].min()
date_max        = df["match_date"].max()
top_event       = df.groupby("event_name")["match_id"].nunique().idxmax()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Partidas", f"{total_matches:,}")
c2.metric("Jugadores únicos", f"{total_players:,}")
c3.metric("Rango de fechas", f"{date_min} → {date_max}")
c4.metric("Evento más frecuente", top_event)

st.markdown("---")

# ---------------------------------------------------------------------------
# 2. TOP JUGADORES POR RATING
# ---------------------------------------------------------------------------
st.subheader("Top 10 jugadores por rating promedio")

tier_options = ["Todos"] + sorted(df["event_tier"].dropna().unique().tolist())
selected_tier = st.selectbox("Filtrar por event_tier", tier_options)

df_filtered = df if selected_tier == "Todos" else df[df["event_tier"] == selected_tier]

top_players = (
    df_filtered.groupby("player_name")
    .agg(
        partidas=("match_id", "nunique"),
        rating=("rating", "mean"),
        kd_ratio=("kd_ratio", "mean"),
        adr=("adr", "mean"),
    )
    .query("partidas >= 3")
    .sort_values("rating", ascending=False)
    .head(10)
    .reset_index()
)
top_players[["rating", "kd_ratio", "adr"]] = top_players[["rating", "kd_ratio", "adr"]].round(3)
st.dataframe(top_players, use_container_width=True, hide_index=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# 3. RENDIMIENTO GANADORES VS PERDEDORES
# ---------------------------------------------------------------------------
st.subheader("Ganadores vs Perdedores — Rendimiento promedio")

win_df = (
    df.groupby("is_winner")[["rating", "adr", "kast_pct"]]
    .mean()
    .reset_index()
)
win_df["Resultado"] = win_df["is_winner"].map({True: "Ganadores", False: "Perdedores"})

metrics = ["rating", "adr", "kast_pct"]
labels  = ["Rating", "ADR", "KAST %"]

cols = st.columns(3)
for col, metric, label in zip(cols, metrics, labels):
    fig = go.Figure(go.Bar(
        x=win_df["Resultado"],
        y=win_df[metric].round(2),
        marker_color=["#2ecc71", "#e74c3c"],
        text=win_df[metric].round(2),
        textposition="outside",
    ))
    fig.update_layout(
        title=label,
        yaxis_title=label,
        height=350,
        showlegend=False,
        margin=dict(t=40, b=20),
    )
    col.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ---------------------------------------------------------------------------
# 4. MAPA DE CALOR DE EQUIPOS — WINRATE
# ---------------------------------------------------------------------------
st.subheader("Winrate por equipo (mín. 5 partidas)")

team_stats = (
    df.groupby("team_name")
    .agg(
        partidas=("match_id", "nunique"),
        ganadas=("is_winner", "sum"),
    )
    .query("partidas >= 5")
    .assign(winrate=lambda x: (x["ganadas"] / x["partidas"] * 100).round(1))
    .sort_values("winrate", ascending=False)
    .reset_index()
)

fig_teams = px.bar(
    team_stats,
    x="team_name",
    y="winrate",
    color="winrate",
    color_continuous_scale="RdYlGn",
    text="winrate",
    labels={"team_name": "Equipo", "winrate": "Winrate (%)"},
    height=450,
)
fig_teams.update_traces(texttemplate="%{text}%", textposition="outside")
fig_teams.update_layout(
    xaxis_tickangle=-45,
    coloraxis_showscale=False,
    margin=dict(b=120),
)
st.plotly_chart(fig_teams, use_container_width=True)
