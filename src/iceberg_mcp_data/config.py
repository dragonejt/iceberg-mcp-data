from dataclasses import dataclass

from cyclopts import Parameter


@Parameter("*")
@dataclass
class PipelineConfig:
    debug: bool = False
