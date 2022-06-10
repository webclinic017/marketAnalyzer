
import json
from requests.structures import CaseInsensitiveDict
import json
import requests
import configparser
def authenticate():
    config = configparser.ConfigParser()
    config.read('config/config_key.properties')
    user=config.get('SUPERSET', "user")
    password= config.get('SUPERSET', "password")

    session = requests.Session()
    r=session.post("http://localhost:8088/api/v1/security/login",json={
      "password":password,
      "provider": "db",
      "refresh": True,
      "username": user}
                  )
    dic=json.loads(r.text)
    token=dic["access_token"]
    headers = CaseInsensitiveDict()

    headers["Authorization"] = "Bearer " + token
    r = session.get("http://localhost:8088/api/v1/security/csrf_token/",headers=headers)

    dic = json.loads(r.text)
    csrf_token = dic["result"]
    headers['X-CSRFToken'] = csrf_token
    return headers,session

