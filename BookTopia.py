import hashlib
import json
from threading import Thread

from pymongo import MongoClient
import requests

client = MongoClient('mongodb://localhost:27017/')
db = client['BookTopia']
Input_ = db['Input_']
collection = db['Data_']

count=0

cookies = {
    '_gid': 'GA1.3.409690163.1716620038',
    '_gac_UA-413837-1': '1.1716620038.CjwKCAjw9cCyBhBzEiwAJTUWNRhcDWSybyho2s0Td9J0CY1yMXxy49kTs1XEQm_esd2_icRE3pGFyRoCISkQAvD_BwE',
    '_gcl_aw': 'GCL.1716620039.CjwKCAjw9cCyBhBzEiwAJTUWNRhcDWSybyho2s0Td9J0CY1yMXxy49kTs1XEQm_esd2_icRE3pGFyRoCISkQAvD_BwE',
    '_gcl_gs': '2.1.k1$i1716620033',
    '_gcl_au': '1.1.1067050692.1716620040',
    'ftr_ncd': '6',
    '_pxvid': '939a14ab-1a63-11ef-a7c4-64024d903113',
    '_ga': 'GA1.1.189287030.1716620038',
    '_uetsid': '90f675b01a6311ef8a6c7555cc77a866',
    '_uetvid': '90f6c4601a6311ef9921bd69e38ce279',
    '_fbp': 'fb.2.1716620041336.669279089',
    'AWSALB': 'aaMydMZQQ4/+IGMoPFteYgqJWm9JD76A2Kdyu1QchmtGGp5K9gmpqUKf6YYGH09vhWE9sq23xHUSBvC32gUBlTbe5rO5L1pfL7TOrinQSqeyOeLXy0ulprcYS/qw',
    'AWSALBCORS': 'aaMydMZQQ4/+IGMoPFteYgqJWm9JD76A2Kdyu1QchmtGGp5K9gmpqUKf6YYGH09vhWE9sq23xHUSBvC32gUBlTbe5rO5L1pfL7TOrinQSqeyOeLXy0ulprcYS/qw',
    'domainCustomerSessionGuid': 'C6B52E44-5689-E815-F24E-24F5B94B41A8',
    'gaUniqueIdentifier': '547B28F9-7563-8D54-8F79-C0D524106C71',
    'JSESSIONID': '5EcRfMGAnWMJI1FSo30CQKAcWatGHCphZLv6dLW1.10.0.103.187',
    '_tt_enable_cookie': '1',
    '_ttp': 'XU7yAO_PJDep_ZH-f-jeU2z_Ys0',
    'FPID': 'FPID2.3.IWt2xeeoGiRaw0AHyJh0SJ0xLoyfTjh83Ddlv01Tr%2BY%3D.1716620038',
    'FPLC': '%2FDxH0YMB77GTt%2BfDDI3LKcV4C%2Bz9SKo%2FVP%2BhjuZD03tg1YM5oazsnWktTxbnkEQyaZsPBm0jm6emxsCZmWuXCkfVRNqoy5KrDRg389xHyGkkcflBQN%2B1CjuWPwQbFA%3D%3D',
    'scarab.visitor': '%227C67EF15B278024E%22',
    '__attentive_id': '7e1e959d683d4786839d0f5e8b3dc817',
    '_attn_': 'eyJ1Ijoie1wiY29cIjoxNzE2NjIwMDQzMzU2LFwidW9cIjoxNzE2NjIwMDQzMzU2LFwibWFcIjoyMTkwMCxcImluXCI6ZmFsc2UsXCJ2YWxcIjpcIjdlMWU5NTlkNjgzZDQ3ODY4MzlkMGY1ZThiM2RjODE3XCJ9In0=',
    '__attentive_cco': '1716620043358',
    '__rtbh.uid': '%7B%22eventType%22%3A%22uid%22%2C%22id%22%3A%22unknown%22%7D',
    '__rtbh.lid': '%7B%22eventType%22%3A%22lid%22%2C%22id%22%3A%22XWZ9KPX2jkIoxbUUXZfV%22%7D',
    '__attentive_pv': '1',
    '__attentive_ss_referrer': 'https://www.google.com/',
    '__attentive_dv': '1',
    'FPGSID': '1.1716620047.1716620111.G-XYG4G317GS.gib-asmy5_s8hLOZPEgArg',
    'forterToken': '2f83e30c4f1b4e3d8e255ff873a0a431_1716620038849__UDF43-m4_6_MYdEibfTa0k%3D-1555-v2',
    'forterToken': '2f83e30c4f1b4e3d8e255ff873a0a431_1716620038849__UDF43-m4_6_MYdEibfTa0k%3D-1555-v2',
    '_rdt_uuid': '1716620041242.f24ace43-874d-440d-b308-c74851cb1efe',
    '_ga_XYG4G317GS': 'GS1.1.1716620040.1.1.1716620422.0.0.461436433',
}

headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'newrelic': 'eyJ2IjpbMCwxXSwiZCI6eyJ0eSI6IkJyb3dzZXIiLCJhYyI6IjY4NzEyIiwiYXAiOiI3MTgzNTkxMTQiLCJpZCI6ImVhYzQxOTQxZjNjZGEwYTEiLCJ0ciI6IjZhNzQ5OTM0M2Y0ZThjYWY4MmUxZmJjZDZiZjM4NTAwIiwidGkiOjE3MTY2MjA0MjI1NzB9fQ==',
    'priority': 'u=1, i',
    'referer': 'https://www.booktopia.com.au/books/hot-price-bestsellers/l115-p1.html?sorter=bestsellers-dsc&gad_source=1&gclid=CjwKCAjw9cCyBhBzEiwAJTUWNRhcDWSybyho2s0Td9J0CY1yMXxy49kTs1XEQm_esd2_icRE3pGFyRoCISkQAvD_BwE',
    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'traceparent': '00-6a7499343f4e8caf82e1fbcd6bf38500-eac41941f3cda0a1-01',
    'tracestate': '68712@nr=0-1-68712-718359114-eac41941f3cda0a1----1716620422570',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    'x-nextjs-data': '1',
}

# MAIN FUNCTION
def Get_Data(start,end):
    print(start)    # Thread start Point
    print(end)      # Thread end Point
    cur=Input_.find({'status':'Pending'}).skip(start).limit(end)
    for i in cur:
        isbn=i['ISBN13']

        params = {
            'keywords': isbn,
            'productType': '917505',
            'pn': '1',
        }

        response1 = requests.get(
            'https://www.booktopia.com.au/_next/data/BiLaGwnyd3BPwc2WwI_bZ/search.json',
            params=params,
            cookies=cookies,
            headers=headers,
        )
        if response1.status_code==200:
            try:
                js1=json.loads(response1.text)
                try:
                    url_slug=js1['pageProps']['__N_REDIRECT']
                except:
                    # print('NOT FOUND')
                    Input_.update_one({'ISBN13': isbn}, {'$set': {'status': 'Not Found'}})
                    continue

                namee=url_slug.split('/')[1]
                typee=url_slug.split('/')[2]

                params = {
                    'productName': namee,
                    'type': typee,
                }

                response = requests.get(
                    f'https://www.booktopia.com.au/_next/data/BiLaGwnyd3BPwc2WwI_bZ{url_slug}.json',
                    params=params,
                    cookies=cookies,
                    headers=headers
                )

                # MAIN DATA JSON

                main_json=json.loads(response.text)

                book_name=main_json['pageProps']['product']['displayName']
                try:
                    author=main_json['pageProps']['product']['contributors'][0]['name']
                except:
                    author=''
                try:
                    original_price=main_json['pageProps']['product']['retailPrice']
                except:
                    original_price=''
                try:
                    discount_price=main_json['pageProps']['product']['salePrice']
                except:
                    discount_price=''
                try:
                    book_type=main_json['pageProps']['product']['bindingFormat']
                except:
                    book_type=''
                #     $.pageProps.product.numberOfPages
                try:
                    isbn10=main_json['pageProps']['product']['isbn10']
                except:
                    isbn10=''
                try:
                    publicationDate=main_json['pageProps']['product']['publicationDate']
                except:
                    publicationDate=''
                try:
                    publisher=main_json['pageProps']['product']['publisher']
                except:
                    publisher=''
                try:
                    numberOfPages=main_json['pageProps']['product']['numberOfPages']
                except:
                    numberOfPages=''

                item={}
                item['ISBN']=isbn
                item['Title of the Book']=book_name
                item['Author/s']=author
                item['Book type']=book_type
                item['Original Price (RRP)']=original_price
                item['Discounted price']=discount_price
                item['ISBN-10']=isbn10
                item['Published Date']=publicationDate
                item['Publisher']=publisher
                item['No. of Pages']=numberOfPages

                combined_data = f"{book_name}{author}{book_type}{original_price}{discount_price}{isbn10}"

                hash_object = hashlib.sha256()
                hash_object.update(combined_data.encode('utf-8'))
                hash_id = hash_object.hexdigest()
                item['Hashid']=hash_id

                try:
                    collection.create_index('Hashid',unique=True)
                    collection.insert_one(item)
                    Input_.update_one({'ISBN13':isbn},{'$set':{'status':'Done'}})
                    print("DATA INSERTED",count)
                except Exception as e:
                    # print(e)
                    Input_.update_one({'ISBN13': isbn}, {'$set': {'status': 'Done'}})
                    print(isbn)
            except Exception as e:
                print(e)
        else:
            print(response1.status_code)
if __name__ == '__main__':
    # RUN IN THREADS

    data=Input_.count_documents({'status':'Pending'})  # FETCH INPUT COUNTS
    run_count = 0
    while data != 0 and run_count < 100:
        total_count = data
        variable_count = total_count // 1        # DEFINE HOW MANY THREADS
        if variable_count == 0:
            variable_count = total_count * 2
        count = 1
        threads = [Thread(target=Get_Data, args=(i, variable_count)) for i in     # FUNCTION CALL
                   range(0, total_count, variable_count)]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        run_count += 1
