from __future__ import annotations

import argparse
import csv
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SKILLS = ROOT / "configs" / "mappings" / "skill_dictionary.csv"
DEFAULT_ALIASES = ROOT / "configs" / "mappings" / "skill_aliases.csv"


class ValidationError(ValueError):
    """Raised when the skill mapping files are inconsistent."""


def _read_csv(path: Path, required_columns: set[str]) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            headers = set(reader.fieldnames or [])
            missing = required_columns - headers
            if missing:
                raise ValidationError(
                    f"{path}: missing required columns: {', '.join(sorted(missing))}"
                )
            rows = []
            for line_number, row in enumerate(reader, start=2):
                if None in row:
                    raise ValidationError(f"{path}:{line_number}: column count does not match header")
                rows.append({key: (value or "").strip() for key, value in row.items()})
            return rows
    except OSError as exc:
        raise ValidationError(f"Cannot read {path}: {exc}") from exc


def _flag(value: str, *, field: str, line: int, path: Path) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes"}:
        return True
    if normalized in {"0", "false", "no"}:
        return False
    raise ValidationError(f"{path}:{line}: {field} must be 0/1 or true/false")


def _alias_key(alias: str) -> str:
    return unicodedata.normalize("NFKC", alias).strip().casefold()


def validate(skills_path: Path = DEFAULT_SKILLS,
             aliases_path: Path = DEFAULT_ALIASES) -> tuple[int, int]:
    skills = _read_csv(skills_path, {"skill_id", "skill_name"})
    aliases = _read_csv(
        aliases_path,
        {"alias", "skill_id", "match_type", "case_sensitive", "requires_context"},
    )

    known_skill_ids: set[str] = set()
    seen_skill_ids: set[str] = set()
    errors: list[str] = []
    for line, skill in enumerate(skills, start=2):
        skill_id = skill["skill_id"]
        if not skill_id:
            errors.append(f"{skills_path}:{line}: skill_id must not be empty")
        elif skill_id in seen_skill_ids:
            errors.append(f"{skills_path}:{line}: duplicate skill_id: {skill_id}")
        else:
            seen_skill_ids.add(skill_id)
            known_skill_ids.add(skill_id)
        if not skill["skill_name"]:
            errors.append(f"{skills_path}:{line}: skill_name must not be empty")

    alias_targets: dict[str, set[str]] = defaultdict(set)
    alias_context: dict[str, list[bool]] = defaultdict(list)
    for line, row in enumerate(aliases, start=2):
        alias, skill_id = row["alias"], row["skill_id"]
        if not alias:
            errors.append(f"{aliases_path}:{line}: alias must not be empty")
            continue
        if skill_id not in known_skill_ids:
            errors.append(f"{aliases_path}:{line}: unknown skill_id: {skill_id}")
            continue
        try:
            _flag(row["case_sensitive"], field="case_sensitive", line=line, path=aliases_path)
            requires_context = _flag(
                row["requires_context"], field="requires_context", line=line,
                path=aliases_path,
            )
        except ValidationError as exc:
            errors.append(str(exc))
            continue

        key = _alias_key(alias)
        alias_targets[key].add(skill_id)
        alias_context[key].append(requires_context)

    for key, targets in alias_targets.items():
        if len(targets) > 1 and not all(alias_context[key]):
            errors.append(
                f"Ambiguous alias {key!r} maps to {', '.join(sorted(targets))}; "
                "set requires_context=1 on every row or map it to one skill"
            )

    if errors:
        raise ValidationError("\n".join(errors))
    return len(skills), len(aliases)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate the skill dictionary and alias mappings."
    )
    parser.add_argument("--skills", type=Path, default=DEFAULT_SKILLS,
                        help="Skill dictionary CSV")
    parser.add_argument("--aliases", type=Path, default=DEFAULT_ALIASES,
                        help="Skill aliases CSV")
    args = parser.parse_args()

    try:
        skill_count, alias_count = validate(args.skills, args.aliases)
    except ValidationError as exc:
        print(f"Skill validation failed:\n{exc}", file=sys.stderr)
        return 1

    print(f"Skill validation passed: {skill_count} skills, {alias_count} aliases.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
