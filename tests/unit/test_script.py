import os
import tempfile

import numpy as np
import pandas as pd
from unittest.mock import patch

import pytest

from src.script import Script


@pytest.mark.type_integration
class TestCaseScript:
    @pytest.fixture
    def dataframe1(self):
        return pd.DataFrame({"A": [1.0, np.nan, 3.0], "B": [4.0, np.nan, 6.0]})

    @pytest.fixture
    def dataframe2(self):
        return pd.DataFrame({"C": [7.0, np.nan, 9.0]})

    @pytest.fixture
    def tmp_dir(self):
        with tempfile.TemporaryDirectory() as directory:
            yield directory

    @pytest.fixture
    def script(self, dataframe1, dataframe2, tmp_dir):
        with patch("src.script.CustomProcessor._load_datasets", return_value={"dataset1": dataframe1, "dataset2": dataframe2}):
            yield Script(datasets_paths={"dataset1": "dataset1.parquet"}, output_path=tmp_dir)

    @pytest.mark.execution_slow
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_run_processor(self, script, tmp_dir):
        expected_df1 = pd.DataFrame({"A": [1.0, 0.0, 3.0], "B": [4.0, 0.0, 6.0], "feature": [5.0, 0.0, 9.0]})
        expected_df2 = pd.DataFrame({"C": [7.0, 0.0, 9.0]})

        script.run()

        assert sorted(os.listdir(tmp_dir)) == ["dataset1.parquet", "dataset2.parquet"]
        dataset1 = pd.read_parquet(os.path.join(tmp_dir, "dataset1.parquet"))
        dataset2 = pd.read_parquet(os.path.join(tmp_dir, "dataset2.parquet"))
        assert dataset1.equals(expected_df1)
        assert dataset2.equals(expected_df2)
