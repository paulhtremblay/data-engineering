from unittest import mock

def func1(arg1:int):
    if arg1 == 0:
        return None
    x = 10/arg1
    return x

def mock_func(*args, **kwargs):
    print('in mock func')

def test_func1():
    func1(1)

@mock.patch('__main__.func1')
def test_func2(m1):
    func1(1)

@mock.patch('__main__.func1', side_effect= mock_func )
def test_func3(m1):
    func1(1)

if __name__ == '__main__':
    func1(1)
    func1(0)
    test_func1()
    test_func2()
    test_func3()
