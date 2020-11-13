from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from src.processor import CustomProcessor


@pytest.mark.type_unit
class TestCaseCustomProcessor:
    @pytest.fixture
    def dataframe1(self):
        return pd.DataFrame({"A": [1, np.nan, 3], "B": [4, np.nan, 6]})

    @pytest.fixture
    def dataframe2(self):
        return pd.DataFrame({"C": [7, np.nan, 9]})

    @pytest.fixture
    def processor(self, dataframe1, dataframe2):
        processor = CustomProcessor(datasets_paths={"dataset1": "dataset1.parquet"})
        with patch.object(processor, "_load_datasets", return_value={"dataset1": dataframe1, "dataset2": dataframe2}):
            yield processor

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_init(self):
        datasets_paths = {"dataset1": "dataset1.parquet"}

        processor = CustomProcessor(datasets_paths=datasets_paths)

        assert processor._datasets_paths == datasets_paths

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_preprocess(self, processor):
        dataframe1 = processor.datasets["dataset1"]
        dataframe2 = processor.datasets["dataset2"]

        assert dataframe1.isnull().values.any()
        assert dataframe2.isnull().values.any()

        processor._preprocess(dataframe1, dataframe2)

        assert not dataframe1.isnull().values.any()
        assert not dataframe2.isnull().values.any()

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_add_feature_column(self, processor):
        dataframe1 = processor.datasets["dataset1"]
        expected_result = pd.DataFrame({"A": [1, np.nan, 3], "B": [4, np.nan, 6], "feature": [5, np.nan, 9]})

        result = processor._add_feature_column(dataframe1)

        assert result.equals(expected_result)

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_calculate(self, processor):
        dataframe1 = processor.datasets["dataset1"]
        dataframe2 = processor.datasets["dataset2"]

        with patch.object(processor, "_preprocess", return_value=(dataframe1, dataframe2)), patch.object(
            processor, "_add_feature_column", return_value=dataframe1
        ):
            result_dataframe1, result_dataframe2 = processor.calculate()

        assert dataframe1.equals(result_dataframe1)
        assert dataframe2.equals(result_dataframe2)
