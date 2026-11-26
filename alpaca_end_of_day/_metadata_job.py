"""Run-scoped catalogue metadata selection and publication without price I/O."""

from libs.alpaca_serving_catalogue import metadata_projection
from libs.alpaca_serving_catalogue.contract import CatalogueError
from libs.common.store_lock import StoreBusyError, operation_lock

from . import DataLibraryError, ReaderBusyError, _metadata_export
from ._catalogue_export import accept_export_inputs


def run(
    data, destination, *, public_ids, symbols, csv_manifest, bloodhound_report, catalogue_only, progress=None
):
    if type(catalogue_only) is not bool:
        raise ValueError("catalogue_only must be a boolean")
    try:
        with operation_lock(data.store, exclusive=False):
            selection = _metadata_export.select(
                data,
                public_ids=public_ids,
                symbols=symbols,
                csv_manifest=csv_manifest,
                bloodhound_report=bloodhound_report,
            )
            keys = {data._records[pid]["group_key"] for pid in selection.public_ids}
            groups = {key: data._groups[key] for key in keys}
            all_online = len(groups) == len(data._groups)
            accept_export_inputs(data, groups)
            if destination is None:
                ready = (
                    {}
                    if catalogue_only
                    else metadata_projection.selected(data.store, groups, bodies=False, all_online=all_online)
                )
                return {
                    **_metadata_export.plan(selection, len(groups), catalogue_only=catalogue_only),
                    "groups_to_prepare": 0 if catalogue_only else len(groups) - len(ready),
                    "receipt_reads": 0,
                    "price_payload_reads": 0,
                    "provider_requests": 0,
                }
            facts = (
                {} if catalogue_only else metadata_projection.require(data.store, groups, all_online=all_online)
            )
            return _metadata_export.write(
                data, destination, selection, facts, catalogue_only=catalogue_only, progress=progress
            )
    except CatalogueError as error:
        raise DataLibraryError(str(error)) from error
    except StoreBusyError as error:
        raise ReaderBusyError("metadata export source store is busy; retry later") from error
