import typing

import pandas as pd

from src.datasets import LazyLoadDatasetsMixin, DumpDatasetsMixin

if typing.TYPE_CHECKING:
    import os


class CustomProcessor(LazyLoadDatasetsMixin, DumpDatasetsMixin):
    def __init__(self, datasets_paths: typing.Dict[str, "os.PathLike"]):
        """
        A processor that computes some datasets and applies some transformation to them.

        :param datasets_paths: Name and path of each input dataset.
        :type datasets_paths: typing.Dict[str, os.PathLike]
        """
        self._datasets_paths = datasets_paths

    def _preprocess(self, dataset1: pd.DataFrame, dataset2: pd.DataFrame) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Apply a preprocessing over given dataset that consists of:
        1. Replace NaN with 0.

        :param dataset1: input dataset.
        :type dataset1: pd.DataFrame
        :param dataset2: input dataset.
        :type dataset2: pd.DataFrame
        :return: modified dataset.
        :rtype: pd.DataFrame
        """
        dataset1.fillna(0, inplace=True)
        dataset2.fillna(0, inplace=True)
        return dataset1, dataset2

    def _add_total_column(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate the sum of first and second columns and add it as a new 'total' column.

        :param dataset: input dataset.
        :type dataset: pd.DataFrame
        :return: modified dataset.
        :rtype: pd.DataFrame
        """
        dataset["total"] = dataset[0] + dataset[1]
        return dataset

    def calculate(self) -> typing.Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Performs all the transformations needed to achieve the result:
        1. Delete old datasets.
        2. Load datasets from files.
        3. Apply a preprocessing to them.
        4. Performs the calculation.
        5. Replace datasets with the results.

        :return: result datasets.
        :rtype: typing.Tuple[pd.DataFrame, pd.DataFrame]
        """
        # Delete old datasets
        del self.datasets

        # Load and get datasets
        dataset1 = self.datasets["dataset1"]
        dataset2 = self.datasets["dataset2"]

        # Apply preprocessing to datasets
        dataset1, dataset2 = self._preprocess(dataset1, dataset2)

        # Make all the transformations
        dataset1 = self._add_total_column(dataset1)

        # Replace datasets with modified
        self.datasets["dataset1"] = dataset1
        self.datasets["dataset2"] = dataset2

        return dataset1, dataset2


if __name__ == "__main__":
    main = CustomProcessor(
        {"complete_by_credit_rci": "data/complete_by_credit_rci.h5", "inq_dates_rci": "data/inq_dates_rci.h5"}
    )

    main.calculate()
