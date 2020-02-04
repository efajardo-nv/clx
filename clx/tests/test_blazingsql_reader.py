# Copyright (c) 2019, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import cudf
import pytest
from pathlib import Path
from clx.io.factory.factory import Factory
from clx.io.reader.blazingsql_reader import BlazingSQLReader

from blazingsql import BlazingContext

test_input_base_path = "%s/input" % os.path.dirname(os.path.realpath(__file__))

expected_df = cudf.DataFrame(
    {
        "firstname": ["Emma", "Ava", "Sophia"],
        "lastname": ["Olivia", "Isabella", "Charlotte"],
        "gender": ["F", "F", "F"],
    }
)

blazing_context = BlazingContext()

@pytest.mark.parametrize("bc", [blazing_context])
@pytest.mark.parametrize("test_input_base_path", [test_input_base_path])
@pytest.mark.parametrize("expected_df", [expected_df])
def test_fetch_data_csv(bc, test_input_base_path, expected_df):
    test_input_path = "%s/person.csv" % (test_input_base_path)
    config = {
        "type": "blazingsql",
        "tables" : [
            {
                "input_path": test_input_path,
                "table_name": "person_csv",
                "header": 0
            }
        ],
        "header": 0,
         "sql": "SELECT * FROM person_csv"
    }
    reader = BlazingSQLReader(config)
    fetched_df = reader.fetch_data()

    assert fetched_df.equals(expected_df)

@pytest.mark.parametrize("bc", [blazing_context])
@pytest.mark.parametrize("test_input_base_path", [test_input_base_path])
@pytest.mark.parametrize("expected_df", [expected_df])
def test_fetch_data_parquet(bc, test_input_base_path, expected_df):
    test_input_path = "%s/person.parquet" % (test_input_base_path)
    config = {
        "type": "blazingsql",
        "input_format": "parquet",
        "tables" : [
            {
                "input_path": test_input_path,
                "table_name": "person_pqt",
                "header": 0
            }
        ],
         "sql": "SELECT * FROM person_pqt"
    }
    reader = BlazingSQLReader(config)
    fetched_df = reader.fetch_data()

    assert fetched_df.equals(expected_df)

@pytest.mark.parametrize("bc", [blazing_context])
@pytest.mark.parametrize("test_input_base_path", [test_input_base_path])
@pytest.mark.parametrize("expected_df", [expected_df])
def test_fetch_data_orc(bc, test_input_base_path, expected_df):
    test_input_path = "%s/person.orc" % (test_input_base_path)
    config = {
        "type": "blazingsql",
        "input_format": "orc",
        "tables" : [
            {
                "input_path": test_input_path,
                "table_name": "person_orc",
                "header": 0
            }
        ],
         "sql": "SELECT * FROM person_orc"
    }
    reader = BlazingSQLReader(config)
    fetched_df = reader.fetch_data()

    assert fetched_df.equals(expected_df)
