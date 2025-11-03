import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

user = os.getenv('USER')
password = os.getenv('PASSWORD')
urlParking = os.getenv('URLPARKING')
urlTrafic = os.getenv('URLTRAFIC')

if not all([user, password, urlParking, urlTrafic]):
    raise EnvironmentError("One or more required environment variables are missing.")

if None in [user, password, urlParking, urlTrafic]:
    raise ValueError("One or more required environment variables are not set properly.")    

list_URL = [urlParking, urlTrafic]

def fetch_parking_data(url, username, password ):
    response = requests.get(url, auth=(username, password))
    if response.status_code != 200:
        return {
            "error": True,
            "status_code": response.status_code,
            "message": response.text
        }
    all_data = response.json()
    parking_values = all_data.get("values")
    return parking_values

if __name__ == "__main__":
    parking_data = fetch_parking_data(urlParking, user, password)
    print(json.dumps(parking_data, indent=2))
    for url in list_URL:
        parking_data = fetch_parking_data(url, user, password)
        print(json.dumps(parking_data, indent=2))
