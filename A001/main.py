import requests
import pandas as pd

url = 'https://servicebus2.caixa.gov.br/portaldeloterias/api/resultados?modalidade=Lotofácil'

r = requests.get(url, verify=False)

r_text = r.text.replace('\\r\\n','')
r_text = r.text.replace('"\r\n}','')
r_text = r.text.replace('{\r\n  "html":','')
df = pd.read_html(r_text)
df1 = df
df=df[0].copy()

