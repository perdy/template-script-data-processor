from unittest.mock import patch, MagicMock, call

import pandas as pd
import pytest

from src.datasets import LazyLoadDatasetsMixin, DumpDatasetsMixin


@pytest.mark.type_unit
class TestCaseLazyLoadDatasetsMixin:
    @pytest.fixture
    def mixin(self):
        return LazyLoadDatasetsMixin()

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_load_datasets(self, mixin):
        load_mock = MagicMock()
        LazyLoadDatasetsMixin._datasets_default_load_function = load_mock

        mixin._load_datasets({"foo": "foo.parquet"})

        assert load_mock.call_args_list == [call(path="foo.parquet")]

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_datasets_getter(self, mixin):
        mixin._datasets_paths = {"foo": "foo.parquet"}
        datasets = {"foo": pd.DataFrame}

        with patch.object(mixin, "_load_datasets", return_value=datasets) as load_mock:
            assert load_mock.call_count == 0
            # Assert load is performed
            assert mixin.datasets == datasets
            assert load_mock.call_count == 1
            # Assert load is performed only once
            assert mixin.datasets == datasets
            assert load_mock.call_count == 1

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_datasets_deleter(self, mixin):
        mixin._datasets_paths = {"foo": "foo.parquet"}
        datasets = {"foo": pd.DataFrame}

        with patch.object(mixin, "_load_datasets", return_value=datasets) as load_mock:
            assert load_mock.call_count == 0
            # Assert load is performed
            assert mixin.datasets == datasets
            assert load_mock.call_count == 1
            # Assert datasets are deleted
            del mixin.datasets
            assert not hasattr(mixin, "_datasets")


@pytest.mark.type_unit
class TestCaseDumpDatasetsMixin:
    @pytest.fixture
    def mixin(self):
        return DumpDatasetsMixin()

    @pytest.mark.execution_fast
    @pytest.mark.priority_high
    @pytest.mark.case_positive
    def test_dump_datasets(self, mixin):
        dataset_mock = MagicMock()
        mixin.datasets = {"bar": dataset_mock}

        mixin.dump_datasets("foo", dump_format="parquet")

        assert dataset_mock.to_parquet.call_args_list == [call("foo/bar.parquet")]

    @pytest.mark.execution_fast
    @pytest.mark.priority_mid
    @pytest.mark.case_negative
    def test_dump_datasets_no_datasets_attribute(self, mixin):
        with pytest.raises(AttributeError):
            mixin.dump_datasets("foo")

    @pytest.mark.execution_fast
    @pytest.mark.priority_mid
    @pytest.mark.case_negative
    def test_dump_datasets_invalid_format(self, mixin):
        dataset_mock = MagicMock()
        dataset_mock.to_wrong.side_effect = AttributeError
        mixin.datasets = {"bar": dataset_mock}

        with pytest.raises(ValueError):
            mixin.dump_datasets("foo", dump_format="wrong")
