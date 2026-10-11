"""Step 5b: interactive dashboard over the gold table.

Run with:  streamlit run dashboard.py
Opens in your browser. Hover any bar or point to see exact values.
"""

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


DB_PATH = Path(__file__).parent / "warehouse.duckdb"
FII_COLOR = "#2a78d6"
DII_COLOR = "#eb6834"


@st.cache_data(ttl=300)
def load_metrics() -> pd.DataFrame:
    # read_only lets the dashboard run while you query in DBeaver too.
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        df = con.execute(
            """
            SELECT g.trade_date, g.fii_net, g.dii_net, g.combined_net,
                   g.nifty_close, g.nifty_change_pct, g.fii_side,
                   g.fii_streak_days, g.fii_net_5d, g.fii_net_20d,
                   g.dii_absorption_ratio, s.flow_source
            FROM gold_daily_metrics AS g
            LEFT JOIN silver_daily AS s USING (trade_date)
            ORDER BY g.trade_date
            """
        ).df()
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    numeric = ["fii_net", "dii_net", "combined_net", "nifty_close", "nifty_change_pct",
               "fii_net_5d", "fii_net_20d", "dii_absorption_ratio"]
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce")
    return df


st.set_page_config(page_title="FII/DII Tracker", layout="wide")
st.title("FII vs DII flows and the Nifty 50")
st.caption("Provisional cash-market data from NSE, Rs crore. Nifty prices from Yahoo Finance. "
           "Not investment advice.")

df = load_metrics()
if df.empty:
    st.warning("No data yet. Run extract_fii_dii.py, extract_nifty.py and load_warehouse.py first.")
    st.stop()

# Filter: how many trading days to show
days = st.radio("Show last", [5, 20, 60, "All"], index=1, horizontal=True,
                format_func=lambda v: v if v == "All" else f"{v} days")
view = df if days == "All" else df.tail(days)
latest = df.iloc[-1]

# Headline tiles for the latest day
st.subheader(f"Latest: {latest['trade_date']:%d %b %Y}")
c1, c2, c3, c4 = st.columns(4)
c1.metric("FII net", f"Rs {latest['fii_net']:,.0f} Cr")
c2.metric("DII net", f"Rs {latest['dii_net']:,.0f} Cr")
c3.metric("Nifty 50", f"{latest['nifty_close']:,.2f}" if pd.notna(latest["nifty_close"]) else "n/a",
          f"{latest['nifty_change_pct']:+.2f}%" if pd.notna(latest["nifty_change_pct"]) else None)
side = "selling" if latest["fii_side"] == "sell" else "buying"
c4.metric("FII streak", f"{int(latest['fii_streak_days'])} days {side}")

# Chart 1: daily flows (grouped bars)
st.subheader("Daily net flows")
flows = view[["trade_date", "fii_net", "dii_net"]].rename(
    columns={"fii_net": "FII net", "dii_net": "DII net"})
st.bar_chart(flows, x="trade_date", y=["FII net", "DII net"],
             color=[FII_COLOR, DII_COLOR], stack=False,
             x_label="Trading day", y_label="Rs crore")

# Chart 2: Nifty close (separate chart, never a second y-axis)
st.subheader("Nifty 50 close")
st.line_chart(view.dropna(subset=["nifty_close"]), x="trade_date", y="nifty_close",
              color="#0b0b0b", x_label="Trading day", y_label="Nifty 50")

# Chart 3: DII absorption ratio on FII selling days
st.subheader("How much FII selling did DIIs absorb?")
st.caption("1.0 = DIIs bought exactly what FIIs sold. Below 1.0, net institutional money left the market.")
absorb = view.dropna(subset=["dii_absorption_ratio"])
st.bar_chart(absorb, x="trade_date", y="dii_absorption_ratio", color=DII_COLOR,
             x_label="Trading day", y_label="DII buying / FII selling")

# Table view with lineage
with st.expander("Data table"):
    table = view.sort_values("trade_date", ascending=False).copy()
    table["trade_date"] = table["trade_date"].dt.date
    st.dataframe(table, hide_index=True, use_container_width=True)