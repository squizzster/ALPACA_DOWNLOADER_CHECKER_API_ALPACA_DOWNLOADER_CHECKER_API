"""Application dispatch for a mixed, ordered Alpaca download action plan."""

from libs.alpaca_data_api_solver import VERSION

from .contract import DownloadError

BAR_STRATEGIES = {"bars_single_range", "bars_multi_range", "bars_boundary_batch"}
METADATA_STRATEGIES = {"asset_lookup", "asset_catalogue", "corporate_actions_batch"}


def execute_plan(plan, *, bars=None, metadata=None):
    """Execute each step completely before the next; any failure stops the plan.

    Results retain step/action positions. Routing and requested slices are in each
    action's deliver array. Clients own HTTP, pagination and exact response parsing.
    """
    if plan.get("schema_version") != VERSION:
        raise DownloadError("unsupported API action-plan version")
    clients = {strategy: bars for strategy in BAR_STRATEGIES}
    clients.update({strategy: metadata for strategy in METADATA_STRATEGIES})
    for step in plan["steps"]:
        for action in step["actions"]:
            if clients.get(action["strategy"]) is None:
                raise DownloadError("no executor for API plan strategy: " + action["strategy"])
    results = []
    for step in plan["steps"]:
        values = []
        for action in step["actions"]:
            single = {"schema_version": VERSION, "steps": [{"actions": [action]}]}
            values.append(clients[action["strategy"]].execute_plan(single))
        results.append(values)
    return results
