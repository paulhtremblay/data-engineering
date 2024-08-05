from unittest import mock

import get_api_data1

class FakeRest:

    def __init__(self):
        self.status_code = 200

    def json(self):
        return {'id':1}


def mock_get_json1():
    return {'id':1}

def mock_rest(*args, **kwargs):
    return FakeRest()

def test_nothing():
    print('in test nothing')

@mock.patch('ex_rest.get_json', side_effect= mock_get_json1 )
def test_get_json_returns_dict1(m1):
    get_api_data1.get_json()

@mock.patch('requests.get', side_effect= mock_rest )
def test_get_json_returns_dict2(m1):
    get_api_data1.get_json()
