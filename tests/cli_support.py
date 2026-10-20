"""Select actual canonical command parsers for focused CLI tests."""

import argparse
from functools import partial
from unittest.mock import patch

from libs import alpaca_cli


def parser_for(*commands):
    parser = alpaca_cli.build_parser()
    defaults = {}
    for command in commands:
        action = next(a for a in parser._actions if isinstance(a, argparse._SubParsersAction))
        defaults[action.dest] = command
        parser = action.choices[command]
    parser.set_defaults(**defaults)
    return parser


def invoke(commands, arguments=None):
    return alpaca_cli.main([*commands, *(arguments or [])])


def initialize_command_registration(case):
    """Explicit prerequisite and isolated registry for mutating command fixtures."""
    from libs.alpaca_lineage_build.universe_update import rebuild_universe

    rebuild_universe(case.store)
    registry = case.store.parent / "command_recovery_registry.csv"
    registry.write_text("HIST\n")
    case.enterContext(patch.object(alpaca_cli, "DEFAULT_REGISTRY", registry))


pricing_main = partial(invoke, ("pricing", "acquire"))
pricing_parser = partial(parser_for, "pricing", "acquire")
lineage_main = partial(invoke, ("lineage", "update"))
lineage_parser = partial(parser_for, "lineage", "update")
reader_main = partial(invoke, ("read",))
export_main = partial(invoke, ("export", "bloodhound"))
