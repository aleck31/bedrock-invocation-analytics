import streamlit as st
import boto3
import pandas as pd
import altair as alt
import time

st.set_page_config(page_title="Bedrock Analytics", page_icon="📊", layout="wide")

st.markdown("""
<div style="display:flex; align-items:center; gap:10px; margin-bottom:8px;">
    <span class="material-symbols-outlined" style="font-size:36px; color:#FF6B35;">monitoring</span>
    <h1 style="margin:0;">Bedrock Invocation Analytics</h1>
</div>""", unsafe_allow_html=True)

# Load Material Symbols icons via Google Fonts
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL@20..48,100..700,0..1" rel="stylesheet" />
<style>
.icon-header { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; }
.icon-header .material-symbols-outlined { font-size: 28px; color: #FF6B35; }
.icon-header h3 { margin: 0; padding: 0; }
.metric-card {
    background: linear-gradient(135deg, #667eea11, #764ba211);
    border-radius: 12px; padding: 16px 20px;
    border-left: 4px solid #667eea;
}
.metric-card .label { font-size: 14px; color: #888; margin-bottom: 4px; display: flex; align-items: center; gap: 6px; }
.metric-card .label .material-symbols-outlined { font-size: 18px; color: #667eea; }
.metric-card .value { font-size: 28px; font-weight: 700; color: #1a1a2e; }
section[data-testid="stSidebar"] .material-symbols-outlined { font-size: 20px; vertical-align: middle; }
</style>
""", unsafe_allow_html=True)

def icon_header(icon: str, title: str):
    st.markdown(f"""
    <div class="icon-header">
        <span class="material-symbols-outlined">{icon}</span>
        <h3>{title}</h3>
    </div>""", unsafe_allow_html=True)

def metric_card(icon: str, label: str, value: str):
    return f"""
    <div class="metric-card">
        <div class="label"><span class="material-symbols-outlined">{icon}</span>{label}</div>
        <div class="value">{value}</div>
    </div>"""

# --- Sidebar ---
with st.sidebar:
    st.header("| Configuration")
    region = st.text_input("Region", value="us-west-2")
    workgroup = st.text_input("Athena Workgroup", value="bedrock-logging-analytics-workgroup")
    database = st.text_input("Athena Database", value="bedrock_analytics")
    time_range = st.selectbox("Time Range", ["1 day", "7 days", "30 days", "90 days"], index=1)
    profiles = boto3.Session().available_profiles
    profile = st.selectbox("AWS Profile", profiles, index=profiles.index("default") if "default" in profiles else 0)

days = int(time_range.split()[0])
date_filter = f"datehour >= date_format(date_add('day', -{days}, now()), '%Y/%m/%d/%H')"

# Price map: (input_price, output_price) per 1K tokens
PRICE_MAP = {
    "claude-3-haiku": (0.00025, 0.00125),
    "claude-3-5-haiku": (0.001, 0.005),
    "claude-3-5-sonnet": (0.003, 0.015),
    "claude-3-opus": (0.015, 0.075),
    "llama3-8b": (0.0003, 0.0006),
    "llama3-70b": (0.00265, 0.0035),
    "mistral-7b": (0.00015, 0.0002),
}


def run_query(sql: str) -> pd.DataFrame:
    session = boto3.Session(profile_name=profile, region_name=region)
    client = session.client("athena")
    resp = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        WorkGroup=workgroup,
    )
    qid = resp["QueryExecutionId"]
    while True:
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            st.error(f"Query failed: {status['QueryExecution']['Status'].get('StateChangeReason', '')}")
            return pd.DataFrame()
        time.sleep(0.5)

    results = client.get_query_results(QueryExecutionId=qid)
    rows = results["ResultSet"]["Rows"]
    if len(rows) <= 1:
        return pd.DataFrame()
    headers = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
    data = [[c.get("VarCharValue", "") for c in r["Data"]] for r in rows[1:]]
    df = pd.DataFrame(data, columns=headers)
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass
    return df


# --- Queries ---
try:
    df_by_model = run_query(f"""
        SELECT modelId, count(*) as invocations,
               sum(input.inputTokenCount) as input_tokens,
               sum(output.outputTokenCount) as output_tokens,
               round(avg(output.outputBodyJson.metrics.latencyMs)) as avg_latency_ms
        FROM invocation_logs WHERE {date_filter}
        GROUP BY modelId ORDER BY input_tokens + output_tokens DESC
    """)

    df_by_caller = run_query(f"""
        SELECT identity.arn as caller, modelId,
               count(*) as invocations,
               sum(input.inputTokenCount) as input_tokens,
               sum(output.outputTokenCount) as output_tokens
        FROM invocation_logs WHERE {date_filter}
        GROUP BY identity.arn, modelId ORDER BY input_tokens + output_tokens DESC
    """)

    df_daily = run_query(f"""
        SELECT substr(datehour, 1, 10) as date, count(*) as invocations,
               sum(input.inputTokenCount) as input_tokens,
               sum(output.outputTokenCount) as output_tokens
        FROM invocation_logs WHERE {date_filter}
        GROUP BY substr(datehour, 1, 10) ORDER BY date
    """)

    df_hourly = run_query(f"""
        SELECT datehour as hour, count(*) as invocations,
               sum(input.inputTokenCount) as input_tokens,
               sum(output.outputTokenCount) as output_tokens
        FROM invocation_logs WHERE {date_filter}
        GROUP BY datehour ORDER BY datehour
    """)

    df_latency = run_query(f"""
        SELECT modelId,
               round(avg(output.outputBodyJson.metrics.latencyMs)) as avg_latency_ms,
               round(min(output.outputBodyJson.metrics.latencyMs)) as min_latency_ms,
               round(max(output.outputBodyJson.metrics.latencyMs)) as max_latency_ms,
               count(*) as invocations
        FROM invocation_logs WHERE {date_filter}
        GROUP BY modelId ORDER BY avg_latency_ms DESC
    """)

    df_slow = run_query(f"""
        SELECT timestamp, modelId, input.inputTokenCount as input_tokens,
               output.outputTokenCount as output_tokens,
               output.outputBodyJson.metrics.latencyMs as latency_ms,
               identity.arn as caller
        FROM invocation_logs
        WHERE {date_filter} AND output.outputBodyJson.metrics.latencyMs > 5000
        ORDER BY latency_ms DESC LIMIT 20
    """)

    # --- Helper: compute cost ---
    def add_cost(df):
        def calc(row):
            for key, (ip, op) in PRICE_MAP.items():
                if key in str(row.get("modelId", "")):
                    return round(row.get("input_tokens", 0) * ip / 1000 + row.get("output_tokens", 0) * op / 1000, 6)
            return round(row.get("input_tokens", 0) * 0.001 / 1000 + row.get("output_tokens", 0) * 0.005 / 1000, 6)
        df["cost_usd"] = df.apply(calc, axis=1)
        return df

    def short_model(df):
        df["model"] = df["modelId"].str.extract(r"([^.]+\.[^:]+)")
        return df

    def short_caller(df):
        df["caller_name"] = df["caller"].str.extract(r"([^/]+)$")
        return df

    # =============================================
    # Section 1: Summary Cards
    # =============================================
    st.markdown("---")
    if not df_by_model.empty:
        df_by_model = add_cost(short_model(df_by_model))
        total_invocations = int(df_by_model["invocations"].sum())
        total_input = int(df_by_model["input_tokens"].sum())
        total_output = int(df_by_model["output_tokens"].sum())
        total_cost = df_by_model["cost_usd"].sum()

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(metric_card("counter_1", "Total Invocations", f"{total_invocations:,}"), unsafe_allow_html=True)
        with c2:
            st.markdown(metric_card("login", "Input Tokens", f"{total_input:,}"), unsafe_allow_html=True)
        with c3:
            st.markdown(metric_card("logout", "Output Tokens", f"{total_output:,}"), unsafe_allow_html=True)
        with c4:
            st.markdown(metric_card("payments", "Estimated Cost", f"${total_cost:,.4f}"), unsafe_allow_html=True)
    else:
        st.info("No data in selected time range")

    # =============================================
    # Section 2: By Model - Token & Cost
    # =============================================
    st.markdown("---")
    icon_header("token", "Token Usage & Cost by Model")
    if not df_by_model.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.caption("Token Usage")
            st.bar_chart(df_by_model.set_index("model")[["input_tokens", "output_tokens"]])
        with col2:
            st.caption("Estimated Cost (USD)")
            st.bar_chart(df_by_model.set_index("model")[["cost_usd"]], color=["#FF6B35"])
        st.dataframe(
            df_by_model[["model", "invocations", "input_tokens", "output_tokens", "cost_usd"]],
            hide_index=True, use_container_width=True
        )

    # =============================================
    # Section 3: By Caller - Token & Cost
    # =============================================
    st.markdown("---")
    icon_header("group", "Token Usage & Cost by Caller")
    if not df_by_caller.empty:
        df_by_caller = add_cost(short_model(short_caller(df_by_caller)))
        caller_agg = df_by_caller.groupby("caller_name")[["input_tokens", "output_tokens", "cost_usd"]].sum()
        col1, col2 = st.columns(2)
        with col1:
            st.caption("Token Usage")
            st.bar_chart(caller_agg[["input_tokens", "output_tokens"]])
        with col2:
            st.caption("Estimated Cost (USD)")
            st.bar_chart(caller_agg[["cost_usd"]], color=["#FF6B35"])
        st.dataframe(
            df_by_caller[["caller_name", "model", "invocations", "input_tokens", "output_tokens", "cost_usd"]],
            hide_index=True, use_container_width=True
        )

    # =============================================
    # Section 4: Trends
    # =============================================
    st.markdown("---")
    icon_header("trending_up", "Trends")
    t1, t2 = st.columns(2)
    with t1:
        st.caption("Daily Trend")
        if not df_daily.empty:
            st.line_chart(df_daily.set_index("date")[["input_tokens", "output_tokens", "invocations"]])
        else:
            st.info("No data")
    with t2:
        st.caption("Hourly Trend")
        if not df_hourly.empty:
            st.line_chart(df_hourly.set_index("hour")[["input_tokens", "output_tokens", "invocations"]])
        else:
            st.info("No data")

    # =============================================
    # Section 5: Performance
    # =============================================
    st.markdown("---")
    icon_header("speed", "Performance")
    col1, col2 = st.columns(2)
    with col1:
        st.caption("Latency by Model (ms)")
        if not df_latency.empty:
            df_latency = short_model(df_latency)
            # Melt to long format for grouped horizontal bar
            melt = df_latency.melt(
                id_vars=["model"],
                value_vars=["min_latency_ms", "avg_latency_ms", "max_latency_ms"],
                var_name="metric", value_name="latency_ms"
            )
            melt["metric"] = melt["metric"].map({
                "min_latency_ms": "Min",
                "avg_latency_ms": "Avg",
                "max_latency_ms": "Max"
            })
            chart = alt.Chart(melt).mark_bar(cornerRadiusEnd=4).encode(
                x=alt.X("latency_ms:Q", title="Latency (ms)"),
                y=alt.Y("model:N", sort="-x", title=None),
                color=alt.Color("metric:N",
                    scale=alt.Scale(
                        domain=["Min", "Avg", "Max"],
                        range=["#4CAF50", "#667eea", "#FF6B35"]
                    ),
                    legend=alt.Legend(title=None, orient="top")
                ),
                yOffset="metric:N",
                tooltip=["model", "metric", "latency_ms"]
            ).properties(height=max(len(df_latency) * 80, 150))
            st.altair_chart(chart, use_container_width=True)
            st.dataframe(df_latency[["model", "invocations", "avg_latency_ms", "min_latency_ms", "max_latency_ms"]], hide_index=True, use_container_width=True)
        else:
            st.info("No data")
    with col2:
        st.caption("High Latency Calls (> 5s)")
        if not df_slow.empty:
            df_slow["caller"] = df_slow["caller"].str.extract(r"([^/]+)$")
            df_slow["model"] = df_slow["modelId"].str.extract(r"([^.]+\.[^:]+)")
            st.dataframe(df_slow[["timestamp", "model", "input_tokens", "output_tokens", "latency_ms", "caller"]], hide_index=True, use_container_width=True)
        else:
            st.success("No high latency calls")

except Exception as e:
    st.error(f"Error: {e}")
    st.info("Please check your AWS configuration in the sidebar.")
