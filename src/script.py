import typing

from src.processor import CustomProcessor

if typing.TYPE_CHECKING:  # noqa
    import os


class Script:
    def __init__(self, datasets_paths: typing.Dict[str, "os.PathLike"], output_path: "os.PathLike"):
        self._datasets_paths = datasets_paths
        self._output_path = output_path

    def run(self):
        """
        Run the processor.
        """
        processor = CustomProcessor(datasets_paths=self._datasets_paths)
        processor.calculate()
        processor.dump_datasets(path=self._output_path)
