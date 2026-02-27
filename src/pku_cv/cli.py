from __future__ import annotations

import argparse

from . import exporter, phase1_discovery, phase2_enrich, phase3_normalize


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pku_cv",
        description="PKU professor pipeline (phase-based)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p1 = sub.add_parser("phase1", help="Discover professor names from school faculty-list pages")
    p1.add_argument("--seed-start", type=int, default=0)
    p1.add_argument("--seed-limit", type=int, default=0)
    p1.add_argument("--max-pages-per-seed", type=int, default=30)
    p1.add_argument("--timeout", type=int, default=25)
    p1.add_argument("--no-resume", action="store_true")
    p1.add_argument("--require-deepseek", action="store_true")

    p2 = sub.add_parser("phase2", help="Enrich professor records via DeepSeek web search")
    p2.add_argument("--limit", type=int, default=0)
    p2.add_argument("--no-resume", action="store_true")

    p3 = sub.add_parser("phase3", help="Normalize school/institute names")
    p3.add_argument("--limit", type=int, default=0)
    p3.add_argument("--no-resume", action="store_true")

    sub.add_parser("export", help="Export final CSV aligned with professors_template.csv")

    pall = sub.add_parser("all", help="Run all phases + export")
    pall.add_argument("--seed-start", type=int, default=0)
    pall.add_argument("--seed-limit", type=int, default=0)
    pall.add_argument("--max-pages-per-seed", type=int, default=30)
    pall.add_argument("--timeout", type=int, default=25)
    pall.add_argument("--limit", type=int, default=0)
    pall.add_argument("--no-resume", action="store_true")
    pall.add_argument("--require-deepseek", action="store_true")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "phase1":
        phase1_discovery.run(
            seed_start=args.seed_start,
            seed_limit=args.seed_limit if args.seed_limit > 0 else None,
            max_pages_per_seed=args.max_pages_per_seed,
            timeout=args.timeout,
            resume=not args.no_resume,
            require_deepseek=args.require_deepseek,
        )
        return

    if args.command == "phase2":
        phase2_enrich.run(
            limit=args.limit if args.limit > 0 else None,
            resume=not args.no_resume,
        )
        return

    if args.command == "phase3":
        phase3_normalize.run(
            limit=args.limit if args.limit > 0 else None,
            resume=not args.no_resume,
        )
        return

    if args.command == "export":
        exporter.run()
        return

    if args.command == "all":
        phase1_discovery.run(
            seed_start=args.seed_start,
            seed_limit=args.seed_limit if args.seed_limit > 0 else None,
            max_pages_per_seed=args.max_pages_per_seed,
            timeout=args.timeout,
            resume=not args.no_resume,
            require_deepseek=args.require_deepseek,
        )
        phase2_enrich.run(
            limit=args.limit if args.limit > 0 else None,
            resume=not args.no_resume,
        )
        phase3_normalize.run(
            limit=args.limit if args.limit > 0 else None,
            resume=not args.no_resume,
        )
        exporter.run()
        return


if __name__ == "__main__":
    main()
