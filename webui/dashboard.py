"""Bedrock Invocation Analytics WebUI — Dashboard."""

from nicegui import ui
from webui import data

VERSION = ""  # Set by index.py


def format_number(n: int) -> str:
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


@ui.page("/")
def dashboard_page():
    ui.dark_mode(False)

    accounts = data.get_accounts()
    state = {
        "account": accounts[0]["key"] if accounts else "",
        "days": 7,
    }

    # ── Header bar ──
    with ui.header().classes("bg-white text-gray-800 shadow-sm items-center px-6"):
        ui.button(icon="menu", on_click=lambda: drawer.toggle()).props("flat round")
        ui.label("Bedrock Invocation Analytics").classes("text-xl font-bold ml-2")
        ui.space()
        ui.button(icon="settings", on_click=lambda: ui.navigate.to("/pricing")).props("flat round").tooltip("Pricing Settings")

    # ── Left drawer (sidebar) ──
    with ui.left_drawer(value=True).classes("bg-gray-50 p-4") as drawer:
        ui.label("Configuration").classes("text-lg font-semibold mb-4")

        if not accounts:
            ui.label("⚠️ No data found").classes("text-red-500")
            ui.label(f"Table: {data.USAGE_TABLE}").classes("text-xs text-gray-400")
            ui.label(f"Region: {data.AWS_REGION}").classes("text-xs text-gray-400")
            return

        # Extract unique accounts and regions
        account_ids = sorted(set(a["account_id"] for a in accounts))
        regions = sorted(set(a["region"] for a in accounts))

        account_select = ui.select(
            {a: a for a in account_ids},
            value=account_ids[0],
            label="Account ID",
        ).classes("w-full")

        region_select = ui.select(
            {r: r for r in regions},
            value=regions[0],
            label="Region",
        ).classes("w-full mt-2")

        days_select = ui.select(
            {1: "Last 24h", 7: "Last 7 days", 30: "Last 30 days", 90: "Last 90 days"},
            value=state["days"],
            label="Time Range",
        ).classes("w-full mt-2")

        ui.separator().classes("my-4")
        ui.label(f"Deployed Region: {data.AWS_REGION}").classes("text-xs text-gray-400")
        ui.label(f"v{VERSION}").classes("text-xs text-gray-400")

    # ── Main content ──
    content = ui.column().classes("w-full max-w-7xl mx-auto p-6 gap-6")

    def refresh():
        state["account"] = f"{account_select.value}#{region_select.value}"
        state["days"] = days_select.value or 7
        content.clear()
        with content:
            render_dashboard(state["account"], state["days"])

    account_select.on_value_change(lambda _: refresh())
    region_select.on_value_change(lambda _: refresh())
    days_select.on_value_change(lambda _: refresh())

    state["account"] = f"{account_select.value}#{region_select.value}"
    with content:
        render_dashboard(state["account"], state["days"])


def render_dashboard(account_region: str, days: int):
    # ── Summary cards ──
    summary = data.get_summary(account_region, days)

    # ── Token Usage & Cost by Model (Chart) ──
    with ui.row().classes("w-full gap-4 flex-wrap"):
        summary_card("Total Invocations", format_number(summary["invocations"]), "call_made", "blue")
        summary_card("Input Tokens", format_number(summary["input_tokens"]), "input", "green")
        summary_card("Output Tokens", format_number(summary["output_tokens"]), "output", "orange")
        summary_card("Estimated Cost", f"${summary['cost_usd']:.4f}", "attach_money", "red")
        summary_card("Avg Latency", f"{summary['avg_latency_ms']}ms", "speed", "purple")

    models = data.get_by_model(account_region, days)
    if models:
        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center px-4 pt-2"):
                ui.label("Token Usage & Cost by Model").classes("text-lg font-semibold")
                ui.space()
                with ui.tabs().props("dense").classes("text-xs") as model_tabs:
                    ui.tab("chart", label="Chart", icon="bar_chart")
                    ui.tab("table", label="Table", icon="table_rows")
            ui.separator()
            with ui.tab_panels(model_tabs, value="chart").classes("w-full max-h-[420px] overflow-auto p-0"):
                with ui.tab_panel("chart").classes("p-2"):
                    model_names = [m["model"].replace("global.", "").replace("anthropic.", "").replace("meta.", "")[:30] for m in models[:15]]
                    ui.echart({
                        "tooltip": {"trigger": "axis"},
                        "legend": {"top": 0},
                        "grid": {"top": 40, "bottom": 70, "left": 60, "right": 60},
                        "xAxis": {"type": "category", "data": model_names, "axisLabel": {"rotate": 40, "interval": 0}},
                        "yAxis": [
                            {"type": "value", "name": "Tokens"},
                            {"type": "value", "name": "Cost ($)"},
                        ],
                        "series": [
                            {"name": "Input Tokens", "type": "bar", "data": [m["input_tokens"] for m in models[:15]]},
                            {"name": "Output Tokens", "type": "bar", "data": [m["output_tokens"] for m in models[:15]]},
                            {"name": "Cost ($)", "type": "bar", "itemStyle": {"color": "#F97316"}, "yAxisIndex": 1, "data": [round(m["cost_usd"], 4) for m in models[:15]]},
                        ],
                    }).classes("w-full h-96")
                with ui.tab_panel("table"):
                    ui.table(
                        columns=[
                            {"name": "model", "label": "Model", "field": "model", "align": "left", "sortable": True},
                            {"name": "invocations", "label": "Calls", "field": "invocations", "sortable": True},
                            {"name": "input_tokens", "label": "Input Tokens", "field": "input_tokens", "sortable": True},
                            {"name": "output_tokens", "label": "Output Tokens", "field": "output_tokens", "sortable": True},
                            {"name": "cost", "label": "Cost ($)", "field": "cost", "sortable": True},
                        ],
                        rows=[{**m, "cost": round(m["cost_usd"], 4)} for m in models],
                    ).classes("w-full")

    # ── Token Usage & Cost by Caller ──
    callers = data.get_by_caller(account_region, days)
    if callers:
        with ui.card().classes("w-full"):
            with ui.row().classes("w-full items-center px-4 pt-2"):
                ui.label("Token Usage & Cost by Caller").classes("text-lg font-semibold")
                ui.space()
                with ui.tabs().props("dense").classes("text-xs") as caller_tabs:
                    ui.tab("chart", label="Chart", icon="bar_chart")
                    ui.tab("table", label="Table", icon="table_rows")
            ui.separator()
            with ui.tab_panels(caller_tabs, value="chart").classes("w-full max-h-[420px] overflow-auto p-0"):
                with ui.tab_panel("chart").classes("p-2"):
                    caller_names = [c["caller"][:25] for c in callers[:15]]
                    ui.echart({
                        "tooltip": {"trigger": "axis"},
                        "legend": {"top": 0},
                        "grid": {"top": 40, "bottom": 70, "left": 60, "right": 60},
                        "xAxis": {"type": "category", "data": caller_names, "axisLabel": {"rotate": 40, "interval": 0}},
                        "yAxis": [
                            {"type": "value", "name": "Tokens"},
                            {"type": "value", "name": "Cost ($)"},
                        ],
                        "series": [
                            {"name": "Input Tokens", "type": "bar", "data": [c["input_tokens"] for c in callers[:15]]},
                            {"name": "Output Tokens", "type": "bar", "data": [c["output_tokens"] for c in callers[:15]]},
                            {"name": "Cost ($)", "type": "bar", "itemStyle": {"color": "#F97316"}, "yAxisIndex": 1, "data": [round(c["cost_usd"], 4) for c in callers[:15]]},
                        ],
                    }).classes("w-full h-96")
                with ui.tab_panel("table"):
                    ui.table(
                        columns=[
                            {"name": "caller", "label": "Caller", "field": "caller", "align": "left", "sortable": True},
                            {"name": "invocations", "label": "Calls", "field": "invocations", "sortable": True},
                            {"name": "input_tokens", "label": "Input Tokens", "field": "input_tokens", "sortable": True},
                            {"name": "output_tokens", "label": "Output Tokens", "field": "output_tokens", "sortable": True},
                            {"name": "cost", "label": "Cost ($)", "field": "cost", "sortable": True},
                        ],
                        rows=[{**c, "cost": round(c["cost_usd"], 4)} for c in callers],
                    ).classes("w-full")

    # ── Usage Trend ──
    trend = data.get_trend(account_region, days)
    if trend:
        with ui.card().classes("w-full p-2"):
            ui.label("Usage Trend").classes("text-lg font-semibold px-2 pt-2")
            ui.echart({
                "tooltip": {"trigger": "axis"},
                "legend": {"top": 0},
                "grid": {"top": 40, "bottom": 30, "left": 60, "right": 60},
                "xAxis": {"type": "category", "data": [t["period"] for t in trend]},
                "yAxis": [
                    {"type": "value", "name": "Invocations"},
                    {"type": "value", "name": "Cost ($)"},
                ],
                "series": [
                    {"name": "Invocations", "type": "bar", "data": [t["invocations"] for t in trend]},
                    {"name": "Cost ($)", "type": "line", "itemStyle": {"color": "#F97316"}, "yAxisIndex": 1, "data": [round(t["cost_usd"], 6) for t in trend]},
                ],
            }).classes("w-full h-80")

def summary_card(title: str, value: str, icon: str, color: str):
    with ui.card().classes("min-w-[150px] flex-1 p-6"):
        with ui.row().classes("items-center gap-2"):
            ui.icon(icon).classes(f"text-xl text-{color}-500")
            ui.label(title).classes("text-sm text-gray-500")
        ui.label(value).classes("text-3xl font-bold mt-2")


# ── Pricing Settings page (placeholder) ──
