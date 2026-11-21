"""CLI entry point shared by developers, Compose, and later Airflow tasks."""

from __future__ import annotations

import argparse
import json
import logging
from typing import Sequence

from . import __version__
from .config import ConfigError, load_config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Electricity pipeline: steps 01--05")
    parser.add_argument("--version", action="version", version=__version__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name, description in (
        ("validate-config", "Validate YAML without connecting to infrastructure"),
        ("init", "Create PostgreSQL/Iceberg tables after the Compose bootstrap helpers"),
        ("check-infra", "Check PostgreSQL, JDBC and Iceberg read/write on HDFS"),
    ):
        command = commands.add_parser(name, help=description)
        command.add_argument("--config", default="configs/local.yaml", help="YAML path relative to the project root")
        command.add_argument("--env-file", default=None, help="Optional .env path; process environment takes precedence")
        if name == "validate-config":
            command.add_argument("--require-connections", action="store_true", help="Also validate required environment variables")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = None
    try:
        settings = load_config(args.config, env_file=args.env_file)
        logging.basicConfig(level=settings["app"]["log_level"],
                            format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        if args.command == "validate-config":
            if args.require_connections:
                settings.validate_connections()
            print("Configuration valid")
        elif args.command == "init":
            from .infrastructure import initialize
            initialize(settings)
            print("Initialization completed")
        elif args.command == "check-infra":
            from .infrastructure import check_infrastructure
            print(json.dumps(check_infrastructure(settings), indent=2))
        return 0
    except KeyboardInterrupt:
        logging.error("Interrupted")
        return 130
    except Exception as exc:
        if settings is not None:
            from .metrics import safe_error
            message = safe_error(exc, settings)
        else:
            message = str(exc) if isinstance(exc, ConfigError) else type(exc).__name__
        logging.error("%s", message)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
