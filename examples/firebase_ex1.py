import os
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

def _init_app():
    path = os.path.join('/home/henry/Downloads/', 
            'my-test-firebase-project-b975f-firebase-adminsdk-iatbj-46aed6c34d.json'
            )
    cred = credentials.Certificate(path)
    firebase_admin.initialize_app(cred)
    return cred

def set_data( collection_name, 
        document_name,
        data):
    db = firestore.client()
    doc_ref = db.collection(collection_name).document(document_name)
    doc_ref.set(data)

def delete_data(collection_name,
        document_name):
    db = firestore.client()
    doc_ref = db.collection(collection_name).document(document_name)
    doc_ref.delete()

def get_data(collection_name,
        document_name
        ):
    db = firestore.client()
    doc_ref_get = db.collection(collection_name).document(document_name)
    doc = doc_ref_get.get()
    if doc.exists:
        return doc.to_dict()
    else:
        return None

def main(verbose = False):
    collection_name = 'users'
    document_name = 'zIEvnxIB7h9AgSBpiNKD'
    
    cred = _init_app()
    delete_data(collection_name = collection_name,
            document_name = document_name
            )
    return
    data = {'Henry':[2015, 2106, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]}
    set_data(
            collection_name = collection_name,
            document_name = document_name,
            data = data
            )
    d_new = get_data(collection_name = collection_name,
            document_name = document_name
            )
    assert data == d_new
    if verbose:
        print('test passed')

if __name__ == '__main__':
    main(verbose = True)
