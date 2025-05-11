import pathlib
from dataclasses import dataclass


@dataclass()
class ServerArguments:
    properties_path: pathlib.Path
