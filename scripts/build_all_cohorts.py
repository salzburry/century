#!/usr/bin/env python3
"""Build the data dictionary for every cohort in packs/cohorts/.

Runs build_dictionary.build_model() once per cohort, then renders
each requested audience from that single model — DB introspection
runs once per cohort, not once per (cohort × audience) pair.

Per-cohort errors are recorded in BUILD_SUMMARY.md without killing
the batch, so a single bad pack doesn't block the rest of the
fleet from shipping.

Usage:
    python scripts/build_all_cohorts.py
    python scripts/build_all_cohorts.py --audiences customer sales
    python scripts/build_all_cohorts.py --cohorts mtc_aat balboa_ckd
    python scripts/build_all_cohorts.py --dry-run
    python scripts/build_all_cohorts.py --out-dir /tmp/batch
"""
from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime as _dt
import io
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKS_DIR = REPO_ROOT / "packs"
COHORTS_DIR = PACKS_DIR / "cohorts"

# Make the repo importable so we can pull build_dictionary's
# internals directly. The v2 module is the authoritative build path
# for stakeholder audiences (sales / customer); the runner inherits
# whatever quirks the live CLI has.
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import importlib.util as _ilu  # noqa: E402

_BD_PATH = REPO_ROOT / "dictionary_v2" / "build_dictionary.py"
_spec = _ilu.spec_from_file_location("build_dictionary", _BD_PATH)
bd = _ilu.module_from_spec(_spec)
sys.modules.setdefault("build_dictionary", bd)
_spec.loader.exec_module(bd)


# --------------------------------------------------------------------- #
# Per-cohort result model — what BUILD_SUMMARY.md is built from.
# --------------------------------------------------------------------- #


@dataclass
class CohortResult:
    cohort: str                          # CLI/filename slug
    schema_name: str = ""
    status: str = "pending"              # ok | error
    error: str = ""
    patient_count: int | None = None
    table_count: int = 0
    variables_total: int = 0
    variables_implemented: int = 0
    warning_count: int = 0
    audiences_built: list[str] = field(default_factory=list)
    output_paths: list[str] = field(default_factory=list)

    @property
    def drop_pct(self) -> float | None:
        """Fraction of variables a stakeholder audience would drop."""
        if not self.variables_total:
            return None
        dropped = self.variables_total - self.variables_implemented
        return 100.0 * dropped / self.variables_total

    @property
    def implemented_pct(self) -> float | None:
        if not self.variables_total:
            return None
        return 100.0 * self.variables_implemented / self.variables_total


# --------------------------------------------------------------------- #
# stderr tee — captures warnings during a single cohort's build while
# still letting them through to the operator's terminal.
# --------------------------------------------------------------------- #


def _as_friendly_path(p: Path) -> str:
    """Render output path as repo-relative when it's under the repo,
    otherwise as the absolute path. The summary prefers compact
    `Output/foo.xlsx` strings but must not crash when the user
    runs with `--out-dir /tmp/anywhere`."""
    try:
        return str(p.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(p.resolve())


class _TeedStderr:
    def __init__(self, real: Any) -> None:
        self._real = real
        self._buf = io.StringIO()

    def write(self, s: str) -> int:
        self._real.write(s)
        self._buf.write(s)
        return len(s)

    def flush(self) -> None:
        self._real.flush()

    def warning_count(self) -> int:
        return self._buf.getvalue().count("[warn]")


# --------------------------------------------------------------------- #
# Single-cohort build: model once, render each requested audience.
# --------------------------------------------------------------------- #


def _build_one_cohort(
    slug: str, audiences: list[str], formats: list[str],
    out_dir: Path, dry_run: bool, conn: Any,
) -> CohortResult:
    result = CohortResult(cohort=slug)

    tee = _TeedStderr(sys.stderr)
    try:
        with contextlib.redirect_stderr(tee):
            model = bd.build_model(slug, conn=conn, dry_run=dry_run)
            result.schema_name = model.schema_name
            result.patient_count = model.summary.patient_count
            result.table_count = len(model.tables)
            result.variables_total = len(model.variables)
            result.variables_implemented = sum(
                1 for v in model.variables if v.implemented == "Yes"
            )
            for aud in audiences:
                filtered = bd.filter_for_audience(
                    model, aud, cohort_slug=slug,
                )
                stem = f"{model.schema_name}_dictionary"
                if aud != "technical":
                    stem += f"_{aud}"
                if "xlsx" in formats:
                    p = out_dir / f"{stem}.xlsx"
                    bd.write_xlsx(filtered, p, audience=aud)
                    result.output_paths.append(_as_friendly_path(p))
                if "html" in formats:
                    p = out_dir / f"{stem}.html"
                    bd.write_html(filtered, p, audience=aud)
                    result.output_paths.append(_as_friendly_path(p))
                # JSON is internal/debug; suppress for stakeholder audiences.
                if "json" in formats and aud not in ("customer", "sales"):
                    p = out_dir / f"{stem}.json"
                    bd.write_json(filtered, p)
                    result.output_paths.append(_as_friendly_path(p))
                result.audiences_built.append(aud)
        result.status = "ok"
    except Exception as exc:    # one bad cohort must not kill the batch
        result.status = "error"
        result.error = (
            f"{type(exc).__name__}: {exc}\n"
            + "".join(traceback.format_exception_only(type(exc), exc)).strip()
        )
    result.warning_count = tee.warning_count()
    return result


# --------------------------------------------------------------------- #
# BUILD_SUMMARY.md renderer
# --------------------------------------------------------------------- #


def _render_summary(
    results: list[CohortResult], audiences: list[str],
    formats: list[str], dry_run: bool, started_at: str,
) -> str:
    ok_count = sum(1 for r in results if r.status == "ok")
    err_count = sum(1 for r in results if r.status == "error")

    lines: list[str] = [
        f"# Build summary",
        "",
        f"- generated: `{started_at}`",
        f"- cohorts: **{len(results)}** "
        f"(**{ok_count}** ok, **{err_count}** error)",
        f"- audiences: `{', '.join(audiences)}`",
        f"- formats: `{', '.join(formats)}`",
        f"- mode: `{'dry-run (no DB)' if dry_run else 'live'}`",
        "",
    ]
    if dry_run:
        lines.append(
            "> **Dry-run note.** Patient counts, top-value columns, "
            "and the `Implemented` flag are all `0` / `No` because "
            "no DB was opened. The `drop%` column reads as 100% for "
            "every cohort here — that's the dry-run signal, not a "
            "real coverage gap. Run without `--dry-run` to get the "
            "live numbers.")
        lines.append("")

    # Cohort table.
    lines.append("## Per-cohort results")
    lines.append("")
    lines.append(
        "Counts come from the canonical model BEFORE any audience "
        "filter (PII, internal scaffolding tables, cohort table-excludes "
        "are not subtracted). `unimplemented%` is the fraction of "
        "variables flagged `Implemented=No` — a coverage signal about "
        "whether the cohort actually carries data for each variable, "
        "not an audience-policy signal."
    )
    lines.append("")
    lines.append(
        "| cohort | status | patients | tables | variables | "
        "implemented | unimplemented% | warnings |"
    )
    lines.append(
        "|---|---|---:|---:|---:|---|---:|---:|"
    )
    for r in sorted(results, key=lambda x: x.cohort):
        if r.status == "error":
            lines.append(
                f"| `{r.cohort}` | ❌ error | — | — | — | — | — | — |"
            )
            continue
        pct_str = (
            f"{r.implemented_pct:.0f}% "
            f"({r.variables_implemented}/{r.variables_total})"
        ) if r.variables_total else "—"
        drop = (
            f"{r.drop_pct:.0f}%" if r.drop_pct is not None else "—"
        )
        patients = f"{r.patient_count:,}" if r.patient_count else "—"
        warn_str = (
            f"⚠ {r.warning_count}" if r.warning_count else "0"
        )
        lines.append(
            f"| `{r.cohort}` | ok | {patients} | {r.table_count} | "
            f"{r.variables_total} | {pct_str} | {drop} | {warn_str} |"
        )
    lines.append("")

    # Errors block.
    error_results = [r for r in results if r.status == "error"]
    if error_results:
        lines.append("## Errors")
        lines.append("")
        for r in error_results:
            lines.append(f"### `{r.cohort}`")
            lines.append("")
            lines.append("```")
            lines.append(r.error)
            lines.append("```")
            lines.append("")

    # High-drop callout — anything over 25% likely needs criteria
    # tightening (discovery report) rather than a real data gap.
    # Skip in dry-run since every cohort would 100%-trigger this.
    high_drop = [
        r for r in results
        if r.status == "ok" and r.drop_pct is not None and r.drop_pct >= 25
    ] if not dry_run else []
    if high_drop:
        lines.append("## High unimplemented% — review variable criteria")
        lines.append("")
        lines.append(
            "Cohorts where ≥25% of variables come back as "
            "`Implemented=No`. This usually means the variable's "
            "broad `criteria:` (or `match:`) didn't hit anything in "
            "this cohort — either the cohort genuinely lacks the "
            "concept, or the criteria pattern is too narrow / "
            "looks at the wrong column. Run discovery to surface "
            "what the cohort actually contains:"
        )
        lines.append("")
        lines.append("```bash")
        for r in sorted(high_drop, key=lambda x: x.drop_pct or 0, reverse=True):
            lines.append(
                f"python dictionary_v2/discover_exact_matches.py "
                f"--cohort {r.cohort}"
            )
        lines.append("```")
        lines.append("")

    # Output index — flat list of files written, grouped by cohort.
    lines.append("## Output files")
    lines.append("")
    for r in sorted(results, key=lambda x: x.cohort):
        if not r.output_paths:
            continue
        lines.append(f"**`{r.cohort}`**")
        for p in r.output_paths:
            lines.append(f"- `{p}`")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


# --------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------- #


def _list_all_cohort_slugs() -> list[str]:
    return sorted(p.stem for p in COHORTS_DIR.glob("*.yaml"))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--cohorts", nargs="+",
        help="Cohort slugs to build (default: every cohort in packs/cohorts/)",
    )
    parser.add_argument(
        "--audiences", nargs="+",
        choices=("technical", "sales", "pharma", "customer"),
        default=["technical", "customer", "sales"],
        help="Audiences to render per cohort (default: technical customer sales)",
    )
    parser.add_argument(
        "--formats", nargs="+",
        choices=("xlsx", "html", "json"),
        default=["xlsx", "html", "json"],
        help="Output formats (json is auto-suppressed for customer / sales)",
    )
    parser.add_argument(
        "--out-dir", default=str(REPO_ROOT / "Output"),
        help="Where the dictionary files + BUILD_SUMMARY.md land",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Skip the DB connection; build pack-only skeletons "
             "(useful for CI / pack-correctness checks)",
    )
    args = parser.parse_args(argv)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    cohorts = args.cohorts or _list_all_cohort_slugs()
    if not cohorts:
        print(f"No cohort packs found in {COHORTS_DIR}", file=sys.stderr)
        return 1

    started_at = _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")
    print(
        f"Building {len(cohorts)} cohort(s) × {len(args.audiences)} "
        f"audience(s) → {out_dir}",
        file=sys.stderr,
    )

    # One DB connection for the whole batch (live runs only).
    conn = None
    if not args.dry_run:
        psycopg = bd._require_psycopg()

        class _NS:
            host = None; port = None; database = None
            user = None; password = None; sslmode = None
        conn = psycopg.connect(**bd.build_conn_kwargs(_NS()))
        conn.autocommit = True

    results: list[CohortResult] = []
    try:
        for slug in cohorts:
            print(f"  building {slug}…", file=sys.stderr)
            results.append(_build_one_cohort(
                slug, args.audiences, args.formats,
                out_dir, args.dry_run, conn,
            ))
    finally:
        if conn is not None:
            conn.close()

    summary_path = out_dir / "BUILD_SUMMARY.md"
    summary_path.write_text(
        _render_summary(
            results, args.audiences, args.formats,
            args.dry_run, started_at,
        ),
        encoding="utf-8",
    )
    print(f"\nWrote {summary_path}", file=sys.stderr)

    # Exit code: 0 iff every cohort succeeded.
    err_count = sum(1 for r in results if r.status == "error")
    if err_count:
        print(
            f"\n❌ {err_count} of {len(results)} cohort(s) failed — "
            f"see {summary_path}",
            file=sys.stderr,
        )
        return 1
    print(
        f"\n✓ all {len(results)} cohort(s) built — see {summary_path}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
