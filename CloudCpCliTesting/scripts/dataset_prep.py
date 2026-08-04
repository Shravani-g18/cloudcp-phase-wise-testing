#!/usr/bin/env python3
"""scripts/dataset_prep.py — Dataset selection and mixed-workload preparation guide.

Helps you choose or compose the right dataset(s) for a CLI test case.

Usage
-----
    # Show info for a specific dataset
    python3 dataset_prep.py --dataset DS-P7-01

    # Show all datasets in a category
    python3 dataset_prep.py --category "Mixed Full-Pipeline"

    # Suggest datasets for a given test tag / group
    python3 dataset_prep.py --for-tag smoke
    python3 dataset_prep.py --for-tag boundary

    # Get a mixed dataset composition guide (~4 GB)
    python3 dataset_prep.py --suggest mixed --total-gb 4

    # List all datasets with their categories
    python3 dataset_prep.py --list

    # Show which datasets are needed for a specific case
    python3 dataset_prep.py --for-case CLI-SMOKE-01

Dataset map: ../dataset_cloudcp/spec_files/dataset_map.json
"""

import argparse
import json
import pathlib
import sys
import textwrap

# Add parent to path
_HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE.parent))
import cli_config as _cfg
import cli_cases as _cases

# ---------------------------------------------------------------------------
# Dataset map helpers
# ---------------------------------------------------------------------------

def _load_map() -> dict:
    try:
        return _cfg.load_dataset_map()
    except FileNotFoundError:
        print(
            f"[warn] dataset_map.json not found at {_cfg.DATASET_MAP_FILE}. "
            "Dataset descriptions will be unavailable.",
            file=sys.stderr,
        )
        return {}


def _print_dataset(ds_id: str, info: dict) -> None:
    print(f"\n  {ds_id}")
    print(f"    Category   : {info.get('category', '?')}")
    print(f"    Subcategory: {info.get('subcategory', '?')}")
    desc = info.get("description", "")
    for line in textwrap.wrap(desc, width=70):
        print(f"    {line}")


# ---------------------------------------------------------------------------
# Mixed dataset composition advisor
# ---------------------------------------------------------------------------

# Tier configuration aligned with config.json BATCH section and bcloud_final_design.md
_TIER_CONFIG = {
    "zero": {
        "size_bytes": 0,
        "description": "0-byte files (zero tier)",
        "spec_example": "DS-P1-01",
        "batch_size": 2000,
        "open_batches": 4,
    },
    "tiny": {
        "size_bytes_range": (1, 1_048_576),
        "description": "1 B – 1 MiB files (tiny tier)",
        "spec_example": "DS-P1-02",
        "batch_size": 511,
        "open_batches": 8,
        "target_size_mb": 256,
    },
    "small": {
        "size_bytes_range": (1_048_576, 67_108_864),
        "description": "1 MiB – 64 MiB files (small tier)",
        "spec_example": "DS-P1-03",
        "batch_size": 317,
        "open_batches": 8,
        "target_size_mb": 2048,
    },
    "medium": {
        "size_bytes_range": (67_108_864, 1_073_741_824),
        "description": "64 MiB – 1 GiB files (medium tier)",
        "spec_example": "DS-P1-04",
        "batch_size": 50,
        "open_batches": 8,
        "target_size_mb": 10240,
    },
    "large": {
        "size_bytes_range": (1_073_741_824, None),
        "description": "≥1 GiB files (large tier)",
        "spec_example": "DS-P1-05",
        "batch_size": 5,
        "open_batches": 8,
        "target_size_mb": 51200,
    },
}


def _suggest_mixed(total_gb: float) -> None:
    """Print a mixed dataset composition guide for the given total size."""
    total_bytes = int(total_gb * 1024 ** 3)

    # Recommended proportions (biased toward tiny/small, matching DS-P12-01 rationale)
    proportions = {
        "zero":   0.00,   # zero files don't contribute size
        "tiny":   0.375,  # ~37.5%
        "small":  0.25,   # ~25%
        "medium": 0.1875, # ~18.75%
        "large":  0.125,  # ~12.5%  (one large file is fine)
    }

    # Representative file sizes (midpoint of each tier)
    rep_sizes = {
        "zero":   0,
        "tiny":   300_000,        # 300 KB
        "small":  20_000_000,     # 20 MB
        "medium": 300_000_000,    # 300 MB
        "large":  2_000_000_000,  # 2 GB
    }

    zero_count = 500  # fixed: enough to test zero-tier batch sealing

    print(f"\nMixed dataset composition guide (~{total_gb:.1f} GB total)")
    print("=" * 58)
    print(
        f"  Based on DS-P12-01 rationale: tiny/small-heavy, "
        f"all tiers present."
    )
    print()
    print(f"  {'Tier':<10}  {'Files':>8}  {'Avg Size':>12}  {'Contribution':>14}")
    print(f"  {'-'*10}  {'-'*8}  {'-'*12}  {'-'*14}")

    actual_total = 0
    print(f"  {'zero':<10}  {zero_count:>8}  {'0 B':>12}  {'0 B':>14}")

    for tier, prop in proportions.items():
        if tier == "zero":
            continue
        tier_bytes = int(total_bytes * prop)
        rep = rep_sizes[tier]
        if rep == 0:
            count = 0
        else:
            count = max(1, tier_bytes // rep)
        actual_size_gb = (count * rep) / 1024 ** 3
        actual_total += count * rep

        if rep >= 1024 ** 3:
            rep_str = f"{rep / 1024**3:.0f} GB"
        elif rep >= 1024 ** 2:
            rep_str = f"{rep / 1024**2:.0f} MB"
        elif rep >= 1024:
            rep_str = f"{rep / 1024:.0f} KB"
        else:
            rep_str = f"{rep} B"

        print(
            f"  {tier:<10}  {count:>8}  {rep_str:>12}  "
            f"~{actual_size_gb:.2f} GB"
        )

    print()
    print(
        f"  Approximate total: ~{actual_total / 1024**3:.2f} GB "
        f"(zero files add count but 0 bytes)"
    )
    print()
    print("  Generation command (approximate):")
    print(
        f"    python3 scripts/dataset_prep.py --suggest mixed --total-gb {total_gb}"
    )
    print()
    print("  Closest pre-built dataset:")
    print(
        f"    DS-P7-01 (~300 GB, 91,320 files) — use a subset via "
        f"datagen --spec DS-P7-01 --max-files <N>"
    )
    print()
    print("  Spec files for custom generation:")
    print(f"    ../dataset_cloudcp/spec_files/DS-P7-01/")
    print()
    print("  Datagen invocation (on bryck host):")
    print(
        f"    /home/bryck/rperiyas/datagen --spec "
        f"../dataset_cloudcp/spec_files/DS-P7-01/<tier>.yaml"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Dataset selection and mixed-workload preparation guide",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--dataset", metavar="ID", help="Show info for a dataset ID")
    g.add_argument("--category", metavar="NAME", help="List datasets in a category")
    g.add_argument("--for-tag", metavar="TAG", help="Show datasets needed for a test tag")
    g.add_argument("--for-case", metavar="ID", help="Show datasets needed for a test case")
    g.add_argument(
        "--suggest", choices=["mixed"],
        help="Suggest a dataset composition (currently: mixed)",
    )
    g.add_argument("--list", action="store_true", help="List all datasets")

    p.add_argument(
        "--total-gb", type=float, default=4.0,
        help="Target total size in GB for --suggest mixed (default: 4)",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    ds_map = _load_map()

    if args.list:
        print(f"\n{'Dataset ID':<14}  {'Category':<32}  {'Subcategory'}")
        print("-" * 80)
        for ds_id, info in ds_map.items():
            if ds_id.startswith("_"):
                continue
            print(
                f"{ds_id:<14}  {info.get('category',''):<32}  "
                f"{info.get('subcategory','')}"
            )
        return 0

    if args.dataset:
        if args.dataset not in ds_map:
            print(f"[error] Dataset {args.dataset!r} not found in dataset_map.json.")
            return 1
        _print_dataset(args.dataset, ds_map[args.dataset])
        return 0

    if args.category:
        found = [
            (ds_id, info)
            for ds_id, info in ds_map.items()
            if not ds_id.startswith("_")
            and args.category.lower() in info.get("category", "").lower()
        ]
        if not found:
            print(f"[warn] No datasets found for category {args.category!r}.")
            return 0
        print(f"\nDatasets in category '{args.category}':")
        for ds_id, info in found:
            _print_dataset(ds_id, info)
        return 0

    if args.for_tag:
        selected = _cases.get_tag(args.for_tag)
        if not selected:
            print(f"[warn] No cases found for tag {args.for_tag!r}.")
            return 0
        needed: dict[str, list[str]] = {}
        for c in selected:
            for ds in c["datasets"]:
                needed.setdefault(ds, []).append(c["id"])
        print(f"\nDatasets needed for tag '{args.for_tag}':")
        for ds_id, case_ids in needed.items():
            print(f"\n  {ds_id}  (used by: {', '.join(case_ids)})")
            if ds_id in ds_map:
                info = ds_map[ds_id]
                print(f"    {info.get('subcategory','')} — {info.get('description','')[:80]}…")
        return 0

    if args.for_case:
        try:
            case = _cases.get_case(args.for_case)
        except KeyError:
            print(f"[error] Case {args.for_case!r} not found.")
            return 1
        print(f"\nDatasets for case {args.for_case}:")
        for ds_id in case["datasets"]:
            if ds_id in ds_map:
                _print_dataset(ds_id, ds_map[ds_id])
            else:
                print(f"  {ds_id} (not in dataset_map.json)")
        return 0

    if args.suggest == "mixed":
        _suggest_mixed(args.total_gb)
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
