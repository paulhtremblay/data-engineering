import requests

def get_request(endpoint:str) -> dict:
    result =  requests.get(endpoint)
    assert result.status_code == 200
    return result.json()

def get_json():
    data = get_request(endpoint = 'https://jsonplaceholder.typicode.com/todos/1')
    assert isinstance(data, dict)
    assert data.get('id') != None

if __name__ == '__main__':
    get_json()
