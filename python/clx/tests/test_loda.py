# Copyright (c) 2020, NVIDIA CORPORATION.
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
import cudf
import cupy
import numpy as np
from clx.analytics.loda import Loda

ld = Loda(n_random_cuts=10, n_bins=None)
x = cupy.random.randint(0, 100, size=(10, 10))

def test_fit():
    ld.fit(x)
    assert ld._histograms is not None
    assert isinstance(
        ld._histograms,
        cupy.core.core.ndarray
    )
    assert cupy.all(ld._histograms > 0)

def test_score():
    scores = ld.score(x)
    assert scores is not None
    assert isinstance(
        scores,
        cupy.core.core.ndarray
    )
    assert cupy.all(scores > 0)

def test_explain():
    explanation = ld.explain(x[0])
    assert explanation is not None
    assert isinstance(
        explanation,
        cupy.core.core.ndarray
    )
    assert cupy.all(explanation >= 0)

