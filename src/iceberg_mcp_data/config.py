from dataclasses import dataclass

from cyclopts import Parameter


@Parameter("*")
@dataclass(frozen=True)
class PipelineConfig:
    debug: bool = False
