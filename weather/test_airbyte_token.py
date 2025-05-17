import requests

AIRBYTE_HOST = "http://13.39.50.171.nip.io"
CONNECTION_ID = "dfe6eae2-7fd3-4114-8ffc-5c78d6757446"
CLIENT_ID = "66d891a9-88a9-4963-8798-2aa3ea54f402"
CLIENT_SECRET = "KzxIM8REdry0ytOGT2CkU4aYWtGkwXhV"


def get_airbyte_token():
    url = f"{AIRBYTE_HOST}/api/v1/applications/token"
    
    payload = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
    }

    headers = {
        "accept": "application/json",
        "content-type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)

    print(f"Status code: {response.status_code}")
    print("Response JSON:")
    print(response.text)

    response.raise_for_status()  # Lanza excepción si hay error HTTP

    return response.json()["access_token"]

if __name__ == "__main__":
    try:
        token = get_airbyte_token()
        print("\n✅ Access token retrieved successfully:")
        print(token)
    except Exception as e:
        print("\n❌ Error getting token:")
        print(e)
