from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True, slots=True)
class Sample:
    """A single FASTQ sample definition."""

    name: str
    r1: Path
    r2: Optional[Path] = None

    @property
    def is_paired(self) -> bool:
        return self.r2 is not None

