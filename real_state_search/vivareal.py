# Imports
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import json
import time

# API VivaReal (Real State Website in Brazil)
url = "https://glue-api.vivareal.com/v2/listings?addressCity=Bras%C3%ADlia&addressLocationId=BR%3EDistrito%20Federal%3ENULL%3EBrasilia&addressNeighborhood&addressState=Distrito%20Federal&addressCountry=Brasil&addressStreet&addressZone&addressPointLat=-15.826691&addressPointLon=-47.92182&business=SALE&facets=amenities&unitTypes=APARTMENT&unitSubTypes=UnitSubType_NONE%2CDUPLEX%2CLOFT%2CSTUDIO%2CTRIPLEX&unitTypesV3=APARTMENT&usageTypes=RESIDENTIAL&listingType=USED&parentId=null&categoryPage=RESULT&includeFields=search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount)%2Cpage%2CseasonalCampaigns%2CfullUriFragments%2Cnearby(search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount))%2Cexpansion(search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount))%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones%2Cphones)%2Cdevelopments(search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount))%2Cowners(search(result(listings(listing(displayAddressType%2Camenities%2CusableAreas%2CconstructionStatus%2ClistingType%2Cdescription%2Ctitle%2CunitTypes%2CnonActivationReason%2CpropertyType%2CunitSubTypes%2Cid%2Cportal%2CparkingSpaces%2Caddress%2Csuites%2CpublicationType%2CexternalId%2Cbathrooms%2CusageTypes%2CtotalAreas%2CadvertiserId%2Cbedrooms%2CpricingInfos%2CshowPrice%2Cstatus%2CadvertiserContact%2CvideoTourLink%2CwhatsappNumber%2Cstamps)%2Caccount(id%2Cname%2ClogoUrl%2ClicenseNumber%2CshowAddress%2ClegacyVivarealId%2Cphones)%2Cmedias%2CaccountLink%2Clink))%2CtotalCount))&size=100&from={}&q&developmentsSize=5&__vt&levels=CITY%2CUNIT_TYPE&ref&pointRadius&isPOIQuery"

headersList = {
 "Accept": "*/*",
 "User-Agent": "Thunder Client (https://www.thunderclient.com)",
 "x-domain": "www.vivareal.com.br" 
}

payload = ""

# Function to make the request with the parameters set
def get_json(url,i,headersList,payload):
    ret = requests.request("GET",url.format(i),data=payload,headers=headersList)
    soup = bs(ret.text,'html.parser')
    return json.loads(soup.text)

#Dataframe to store all results
df = pd.DataFrame(columns=['description','address','neighborhood','area','bedrooms','suites','wc','parking_spots','value','condo_fee','df_tax','aptlink'])

# Scraping all the pages
apart_id = 0
json_data = get_json(url,apart_id,headersList,payload)
json_tree = json_data['search']['result']['listings']
while len(json_tree) > 0:
    qty = len(json_tree)
    print(f'Quantity of apartaments:{qty} | Total:{apart_id}') #log
    for i in range(0,qty):
        json_apart = json_data['search']['result']['listings'][i]['listing']
        try:
            description = json_apart['title']
        except:
            description = '-'
        try:
            address = json_apart['address']['street']
        except:
            address = '-'
        try:
            neighborhood = json_apart['address']['neighborhood']
        except:
            neighborhood = '-'
        try:
            area = json_apart['totalAreas'][0]
        except:
            area = '-'
        try:
            bedrooms = json_apart['bedrooms'][0]
        except:
            bedrooms = '-'
        try:
            suites = json_apart['suites'][0]
        except:
            suites = '-'
        try:
            wc = json_apart['bathrooms'][0]
        except:
            wc = '-'
        try:
            parking_spots = json_apart['parkingSpaces'][0]
        except:
            parking_spots = '-'
        try:
            value = json_apart['pricingInfos'][0]['price']
        except:
            value = '-'
        try:
            condo_fee = json_apart['pricingInfos'][0]['monthlyCondoFee']
        except:
            condo_fee = '-'
        try:
            df_tax = json_apart['pricingInfos'][0]['yearlyIptu']
        except:
            df_tax = '-'
        try:
            aptlink = json_apart['link']['href']
        except:
            aptlink = '-'
        
        df.loc[df.shape[0]] = [description,address,neighborhood,area,bedrooms,suites,wc,parking_spots,value,condo_fee,df_tax,aptlink]
    # Next Request
    apart_id = apart_id + qty
    time.sleep(1)
    json_data = get_json(url,apart_id,headersList,payload)
    json_tree = json_data['search']['result']['listings']

df.to_csv('apartments_brasilia.csv',sep=';',index=False)
print('All results were saved in apartments_brasilia.csv')

