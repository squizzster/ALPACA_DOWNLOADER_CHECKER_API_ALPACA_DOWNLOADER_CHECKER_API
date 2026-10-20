"""Structured fixtures for retained legacy algorithms whose CLI was retired."""

from argparse import Namespace


def legacy_arguments(**kwargs):
    return Namespace(
        **{
            **dict(
                symbols=None,
                symbol_file=None,
                symbol=None,
                adjustment=None,
                force=False,
                repair_orphaned_download_pair=False,
                adaptive_adjustments=False,
                workers=3,
                include_skipped_symbols=False,
                request_end_timestamp=None,
            ),
            **kwargs,
        }
    )
