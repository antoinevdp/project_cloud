import requests
import json

url = "https://data.grandlyon.com/fr/datapusher/ws/rdata/pvo_patrimoine_voirie.pvotrafic/all.json?maxfeatures=-1&start=1&filename=etat-trafic-metropole-lyon-disponibilites-temps-reel"

payload = json.dumps({
  "text": "i love you chicken uggets"
})
headers = {
  '': 'fa19ab3d05b247d5abad5353ff1eaf8d',
  'Content-Type': 'application/json',
  'Authorization': 'Basic anVsaWVuLnRyZW1vbnQtcmFpbWlAZWZyZWkubmV0Om5UTGVMRHN4NDdtUlljVA==',
  'Cookie': 'GS_FLOW_CONTROL=GS_CFLOW_-6ccaa008:199f0ccd4d8:702e; consent=true'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)
