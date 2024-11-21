import pytest
import os
import sys

CURR_DIR = os.path.dirname(os.path.abspath(__file__))
ONE_UP = os.path.split(CURR_DIR)[1]
sys.path.append(CURR_DIR)
import is_outlier
from is_outlier import BadDataException

def test_raises_error_if_element_wrong_size():
    with pytest.raises(BadDataException) as error:
        is_outlier.is_outlier(element = [1])

def test_is_outlier_returns_true():
        data = [   1.4,
            4.7,
            7.9,
            9.2,
            11.7,
            14.2,
            15.4,
            16.7,
            20.2,
            20.2,
            25.6,
            27.9,
            28.6,
            32.1,
            36.2,
            35.7,
            37.8,
            42.0,
            44.6,
            45.6,
            48.7,
            51.1,
            53.0,
            54.3,
            58.0,
            59.1,
            62.7,
            63.2,
            65.3,
            83.0]
        is_o = is_outlier.is_outlier(element = data)
        assert is_o == True

