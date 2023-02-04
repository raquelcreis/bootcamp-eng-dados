# Imports
import requests
import pandas as pd
import collections
from IPython.display import display

# UrL Lotofácil Brazil
url = 'https://servicebus2.caixa.gov.br/portaldeloterias/api/resultados?modalidade=Lotofácil'

#Request
r = requests.get(url, verify=False)

# Html Cleaning
r_text = r.text.replace('\\r\\n','')
r_text = r.text.replace('"\r\n}','')
r_text = r.text.replace('{\r\n  "html":','')

# Dataframe
df = pd.read_html(r_text)
df1 = df #safety copy
df=df[0].copy()

# Cleaning
df.columns = [ i.replace('\\r\\n','') for i in df.columns]
df = df[df['Bola1'] == df['Bola1']]

# Support Lists
nr_pop = list(range(1, 26))
nr_even = list(range(2,25,2))
nr_odd = list(range(1,26,2))
nr_prime = [2, 3, 5, 7, 11, 13, 17, 19, 23]

comb = []
full = []

lst_balls = ['Bola1', 'Bola2', 'Bola3', 'Bola4', 'Bola5',
              'Bola6', 'Bola7', 'Bola8', 'Bola9', 'Bola10', 'Bola11', 'Bola12',
              'Bola13', 'Bola14', 'Bola15']

# Combination
for index,row in df.iterrows():
    v_even = 0
    v_odd = 0
    v_prime = 0
    for ball in lst_balls:
        if row[ball] in nr_even:
            v_even += 1
        if row[ball] in nr_odd:
            v_odd += 1
        if row[ball] in nr_prime:
            v_prime += 1
    comb.append(str(v_even) + 'p-' + str(v_odd) + 'i-'+str(v_prime)+'np')

# Full List (w all numbers)
for ball in lst_balls: 
    temp = df[ball].explode().to_list() 
    full.extend(temp) 

#Freq Dataframe
def df_freq(lis,id):
    freq = collections.Counter(lis)
    result = pd.DataFrame(freq.items(),columns=[id,'Freq'])
    result['P_freq'] = result['Freq']/result['Freq'].sum()
    result = result.sort_values(by='P_freq',ascending=False)
    result.reset_index(drop=True,inplace=True)
    return result

df_comb = df_freq(comb,'Comb')
df_full = df_freq(full,'Num')

print("The Top 5 Most Frequent Values are:\n")
display(df_full.head())
print("\nThe Bottom 5 Least Frequent Values are:\n")
display(df_full.tail())
print("\nThe Top 5 Most Frequent Combination of Odds, Evens and Primes are:\n")
display(df_comb.head())