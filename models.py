from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass(frozen=True, slots=True)
class Sample:
    """
    Represents a single sequencing sample.

    - Single-end: only r1 is set
    - Paired-end: both r1 and r2 are set
    """

    name: str
    r1: Path
    r2: Path | None = None

    @property
    def paired(self) -> bool:
        return self.r2 is not None

    def get_paths(self) -> list[Path]:
        return [self.r1] if not self.r2 else [self.r1, self.r2]
