#!/usr/bin/env python3
"""Run script.
"""
import logging
import os
import sys

from clinner.command import Type as CommandType
from clinner.command import command
from clinner.run import Main

from src.script import Script

logger = logging.getLogger("cli")


@command(
    command_type=CommandType.PYTHON,
    args=(
        (("-d", "--dataset"), {"help": "Dataset file path", "nargs": "+"}),
        (("output_path",), {"help": "Output path"}),
    ),
    parser_opts={"help": "Run script"},
)
def run(*args, **kwargs):
    try:
        datasets_paths = {os.path.splitext(os.path.basename(x))[0]: x for x in kwargs["dataset"]}
        Script(datasets_paths=datasets_paths, output_path=kwargs["output_path"]).run()
    except Exception as e:
        logger.exception(str(e))


if __name__ == "__main__":
    sys.exit(Main().run())
