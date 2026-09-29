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
    - Any additional FASTQs associated with the sample are stored in additional_reads.
    """

    name: str
    r1: Path
    r2: Path | None = None
    project: str | None = None
    additional_reads: tuple[Path, ...] = ()

    @property
    def paired(self) -> bool:
        return self.r2 is not None

    def get_paths(self) -> list[Path]:
        paths = [self.r1]

        if self.r2 is not None:
            paths.append(self.r2)

        paths.extend(self.additional_reads)

        return paths
