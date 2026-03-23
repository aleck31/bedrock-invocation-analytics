"""Pricing Settings page."""

from nicegui import ui
from webui import data


@ui.page("/pricing")
def pricing_page():
    ui.dark_mode(False)
    with ui.header().classes("bg-white text-gray-800 shadow-sm items-center px-6"):
        ui.button(icon="arrow_back", on_click=lambda: ui.navigate.to("/")).props("flat round")
        ui.label("Pricing Settings").classes("text-xl font-bold ml-2")
        ui.space()
        sync_info = data.get_pricing_sync_info()
        if sync_info:
            synced_at = sync_info.get("synced_at", "")
            updated = int(sync_info.get("models_updated", 0))
            skipped = int(sync_info.get("models_skipped", 0))
            ui.label(f"Last sync: {synced_at}  |  {updated} updated, {skipped} unchanged").classes("text-sm text-gray-500")

    models = data.get_all_pricing()

    with ui.column().classes("max-w-6xl mx-auto p-6 w-full"):
        with ui.row().classes("w-full items-center justify-between mb-2"):
            ui.label(f"{len(models)} models").classes("text-sm text-gray-500")
            search = ui.input(placeholder="Filter models...").props("dense outlined clearable").classes("w-64")

        columns = [
            {"name": "model_id", "label": "Model ID", "field": "model_id", "align": "left", "sortable": True},
            {"name": "input_per_1k", "label": "Input $/1K tokens", "field": "input_per_1k", "sortable": True},
            {"name": "output_per_1k", "label": "Output $/1K tokens", "field": "output_per_1k", "sortable": True},
            {"name": "effective_date", "label": "Effective Date", "field": "effective_date", "sortable": True},
            {"name": "source", "label": "Source", "field": "source", "sortable": True},
        ]
        rows = [{
            "model_id": m["model_id"],
            "input_per_1k": f'{m["input_per_1k"]:.6f}',
            "output_per_1k": f'{m["output_per_1k"]:.6f}',
            "effective_date": m["effective_date"],
            "source": m["source"],
        } for m in models]

        table = ui.table(columns=columns, rows=rows, row_key="model_id", pagination=20).classes(
            "w-full"
        ).props('dense flat')
        search.bind_value_to(table, "filter")
