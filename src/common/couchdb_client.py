import requests

def fetch_data_from_couchdb(url, db_name):
    # db_mappings = {
    #     "channel1_orderline-steps": "channel1_orderline-steps",
    #     "invited_suppliers": "invited-suppliers"
    # }
    # db_name = db_mappings.get(db_name, db_name)
    full_url = f"{url}/{db_name}/_all_docs?include_docs=true"
    response = requests.get(full_url)
    if response.status_code == 200:
        return response.json()['rows']
    else:
        raise Exception(f"Failed to fetch data from CouchDB: {response.text}")