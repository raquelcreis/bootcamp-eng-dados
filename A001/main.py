import requests
import pandas as pd

url = 'https://servicebus2.caixa.gov.br/portaldeloterias/api/resultados?modalidade=Lotofácil'

r = requests.get(url)

r_text = r.text.replace('\\r\\n','')
r_text = r.text.replace('"\r\n}','')
r_text = r.text.replace('{\r\n  "html":','')