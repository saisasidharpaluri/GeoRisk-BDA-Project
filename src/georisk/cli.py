"""Unified command-line interface for local GeoRisk workflows."""

from __future__ import annotations

import argparse
from dataclasses import asdict
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Sequence

from georisk.config import GeoRiskConfig, load_env_file
from georisk.logging_utils import configure_logging


def _spark_available() -> bool:
    try:
        importlib.import_module("pyspark")
    except ImportError:
        return False
    return True


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="georisk", description=__doc__)
    parser.add_argument(
        "--env-file",
        default=argparse.SUPPRESS,
        help="dotenv file (default: GEORISK_ENV_FILE or .env)",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    descriptions = {
        "generate": "Generate deterministic scenario JSONL files",
        "replay": "Replay input JSONL to atomic local stream batches",
        "process": "Process input JSONL into risk and density outputs",
        "run": "Run the complete local pipeline and acceptance checks",
        "validate": "Validate a persisted report and its artifact hashes",
        "compare-resolutions": "Compare generated scenario coverage at H3 resolutions 7 and 8",
        "dashboard": "Launch the Streamlit dashboard",
    }
    if _spark_available():
        descriptions["spark-stream"] = "Process local stream batches with Spark"
    for name, description in descriptions.items():
        command = commands.add_parser(name, help=description, description=description)
        command.add_argument("--env-file", default=argparse.SUPPRESS)

        def option(flag: str, env: str, *, aliases: tuple[str, ...] = (), **kwargs: Any) -> None:
            command.add_argument(
                flag,
                *aliases,
                dest=env,
                default=argparse.SUPPRESS,
                help=f"overrides {env}",
                **kwargs,
            )

        option("--log-level", "GEORISK_LOG_LEVEL")
        if name in {"generate", "run", "compare-resolutions"}:
            option("--seed", "GEORISK_SEED", type=int)
            option("--vessel-count", "GEORISK_VESSEL_COUNT", aliases=("--vessels",), type=int)
            option("--duration-minutes", "GEORISK_SCENARIO_DURATION_MINUTES", type=int)
            option("--interval-seconds", "GEORISK_TELEMETRY_INTERVAL_SECONDS", type=int)
        if name in {"generate", "replay", "process", "run", "dashboard"}:
            aliases = ("--output-dir",) if name == "generate" else ()
            option("--input-dir", "GEORISK_INPUT_DIR", aliases=aliases)
        if name in {"process", "run", "dashboard"}:
            option("--processed-dir", "GEORISK_OUTPUT_DIR", aliases=("--output-dir",))
        if name in {"process", "run", "dashboard", "spark-stream"}:
            option("--resolution", "GEORISK_RESOLUTION", type=int)
        if name in {"process", "run", "dashboard", "spark-stream", "compare-resolutions"}:
            option("--coverage-mode", "GEORISK_H3_COVERAGE_MODE", choices=("overlap", "center"))
        if name in {"process", "run", "spark-stream"}:
            option("--window-minutes", "GEORISK_DENSITY_WINDOW_MINUTES", type=int)
            option("--allowed-lateness-minutes", "GEORISK_ALLOWED_LATENESS_MINUTES", type=int)
        if name in {"process", "run"}:
            option(
                "--strict-validation",
                "GEORISK_STRICT_VALIDATION",
                action=argparse.BooleanOptionalAction,
            )
        if name in {"run", "validate", "compare-resolutions"}:
            option("--report-path", "GEORISK_REPORT_PATH", aliases=("--report",))
        if name in {"run", "dashboard"}:
            option("--route-risk-threshold", "GEORISK_ROUTE_RISK_THRESHOLD", type=float)
            option("--route-risk-penalty", "GEORISK_ROUTE_RISK_PENALTY", type=float)
        if name == "run":
            option("--lake-dir", "GEORISK_LAKE_DIR")
            option("--delay-model", "GEORISK_DELAY_MODEL", choices=("linear", "gbt"))
            option("--evaluation-fraction", "GEORISK_EVALUATION_FRACTION", type=float)
            option("--delay-noise-std-hours", "GEORISK_DELAY_NOISE_STD_HOURS", type=float)
            option(
                "--acceptance-required",
                "GEORISK_ACCEPTANCE_REQUIRED",
                action=argparse.BooleanOptionalAction,
            )
        if name in {"replay", "spark-stream"}:
            option("--stream-dir", "GEORISK_STREAM_DIR")
        if name == "replay":
            option("--speed", "GEORISK_REPLAY_SPEED_FACTOR", type=float)
        if name == "spark-stream":
            option("--output-dir", "GEORISK_SPARK_OUTPUT_DIR")
            option("--checkpoint-dir", "GEORISK_SPARK_CHECKPOINT_DIR")
            command.add_argument(
                "--watch",
                action="store_true",
                help="keep watching for batches instead of draining available input",
            )
    return parser


def _scenario(settings: GeoRiskConfig) -> dict[str, list[dict[str, Any]]]:
    from georisk.ingestion.scenario import ScenarioConfig, generate_scenario

    return generate_scenario(
        ScenarioConfig(
            seed=settings.seed,
            vessel_count=settings.vessel_count,
            duration_minutes=settings.scenario_duration_minutes,
            interval_seconds=settings.telemetry_interval_seconds,
        )
    )


def _dispatch(
    args: argparse.Namespace, settings: GeoRiskConfig, environment: dict[str, str]
) -> int:
    command = args.command
    status = 0
    result: Any
    if command == "generate":
        from georisk.ingestion.generator import write_jsonl

        result = asdict(write_jsonl(_scenario(settings), settings.input_dir))
    elif command == "replay":
        from georisk.ingestion.file_stream import FileStreamPublisher
        from georisk.ingestion.generator import replay_events

        events = {}
        for key, filename in (
            ("telemetry", "vessel-telemetry.jsonl"),
            ("weather_alerts", "weather-alerts.jsonl"),
        ):
            path = settings.input_dir / filename
            events[key] = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        with FileStreamPublisher(settings.stream_dir) as publisher:
            result = asdict(
                replay_events(events, publisher, speed_factor=settings.replay_speed_factor)
            )
        status = int(result["rejected"] > 0)
    elif command == "process":
        from georisk.processing.stream_job import run_streaming_job

        result = run_streaming_job(
            dict(
                input_dir=settings.input_dir,
                output_dir=settings.processed_dir,
                resolution=settings.resolution,
                coverage_mode=settings.coverage_mode,
                window_minutes=settings.density_window_minutes,
                allowed_lateness_minutes=settings.allowed_lateness_minutes,
                strict=settings.strict_validation,
            )
        )
    elif command == "run":
        from georisk.pipeline import run_end_to_end

        result = run_end_to_end(settings)
        status = int(settings.acceptance_required and not result["acceptance"]["passed"])
    elif command == "validate":
        from georisk.operations import validate_report

        result = validate_report(settings.report_path)
        status = int(not result["passed"])
    elif command == "compare-resolutions":
        from georisk.analysis.resolution_comparison import compare_resolutions

        events = _scenario(settings)
        result = compare_resolutions(
            events["telemetry"], events["weather_alerts"], coverage_mode=settings.coverage_mode
        )
        destination = settings.report_path.parent / "resolution-comparison.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    elif command == "dashboard":
        from georisk.route_dashboard import dashboard

        theme = Path(dashboard.__file__).resolve().with_name("theme.toml")
        environment.setdefault("STREAMLIT_THEME_BASE", str(theme))
        return subprocess.run(
            [sys.executable, "-m", "streamlit", "run", str(Path(dashboard.__file__).resolve())],
            env=environment,
            check=False,
        ).returncode
    else:
        from georisk.processing.spark_stream_job import run_stream
        from georisk.spark_session import get_spark

        spark = get_spark("georisk-stream")
        try:
            query = run_stream(
                spark,
                settings.stream_dir,
                settings.spark_output_dir,
                settings.spark_checkpoint_dir,
                resolution=settings.resolution,
                coverage_mode=settings.coverage_mode,
                window_minutes=settings.density_window_minutes,
                allowed_lateness_minutes=settings.allowed_lateness_minutes,
                available_now=not args.watch,
            )
            try:
                query.awaitTermination()
            finally:
                if query.isActive:
                    query.stop()
        finally:
            spark.stop()
        return 0
    print(json.dumps(result, indent=2, sort_keys=True))
    return status


def main(argv: Sequence[str] | None = None) -> int:
    """Run one subcommand; flags override shell values, which override dotenv."""
    args = _parser().parse_args(argv)
    try:
        env_file = Path(getattr(args, "env_file", os.environ.get("GEORISK_ENV_FILE", ".env")))
        if env_file.is_file():
            load_env_file(env_file)
        environment = dict(os.environ)
        environment["GEORISK_ENV_FILE"] = str(env_file)
        environment.update(
            {key: str(value) for key, value in vars(args).items() if key.startswith("GEORISK_")}
        )
        settings = GeoRiskConfig.from_env(environment)
        configure_logging(settings.log_level)
        return _dispatch(args, settings, environment)
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"georisk: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


def entry() -> None:
    """Installed console entry point."""
    raise SystemExit(main())


if __name__ == "__main__":
    entry()
