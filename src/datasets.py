import os
import typing

import pandas as pd


class LazyLoadDatasetsMixin:
    """
    Mixin that provides a lazy way to load datasets from multiple files.

    This mixin expects an attribute named '_datasets_path' that should be a dictionary containing each dataset name and
    its file path.

    The function used to load all datasets can be set
    """

    _datasets_default_load_function = pd.read_parquet
    _datasets_paths = (
        None
    )  # type: typing.Dict[str, typing.Union[os.PathLike, typing.Tuple[os.PathLike, typing.Callable]]]

    def _load_datasets(
        self, datasets_paths: typing.Dict[str, typing.Union[os.PathLike, typing.Tuple[os.PathLike, typing.Callable]]],
    ) -> typing.Dict[str, pd.DataFrame]:
        paths = {
            name: (path[0], path[1])
            if isinstance(path, tuple)
            else (path, LazyLoadDatasetsMixin._datasets_default_load_function)
            for name, path in datasets_paths.items()
        }

        return {name: load_function(path=path) for name, (path, load_function) in paths.items()}

    @property
    def datasets(self) -> typing.Dict[str, pd.DataFrame]:
        if not hasattr(self, "_datasets") and self._datasets_paths:
            self._datasets = self._load_datasets(self._datasets_paths)

        return self._datasets

    @datasets.deleter
    def datasets(self):
        if hasattr(self, "_datasets"):
            del self._datasets


class DumpDatasetsMixin:
    """
    Mixin that provides an unified way to dump all datasets into multiple files.
    """

    _datasets_default_dump_format = "parquet"

    def dump_datasets(self, path: os.PathLike = None, dump_format: str = None) -> None:
        """
        Dump all datasets into files under give path and using specified format.

        :param path: path where all datasets files will be stored.
        :type path: os.PathLike
        :param dump_format: dataframe dump format, e.g: 'parquet'
        :type dump_format: str
        """
        if not hasattr(self, "datasets"):
            raise AttributeError("datasets attribute not found")

        if dump_format is None:
            dump_format = self._datasets_default_dump_format

        try:
            for name, dataset in self.datasets.items():
                getattr(dataset, f"to_{dump_format}")(os.path.join(path, f"{name}.{dump_format}"))
        except AttributeError:
            raise ValueError(f"'{dump_format}' is not a valid dump format")
