import threading
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import time

if not os.path.exists('./futures_data'):
    os.mkdir('./futures_data')

start = time.time()


def crawl(data):
    Day = data.strftime('%Y%m%d')
    print('crawling' + ' ' + Day)
    r = requests.get(
        'https://www.taifex.com.tw/cht/3/futContractsDate?queryDate={}%2F{}%2F{}'.format(data.year, data.month,
                                                                                         data.day))
    soup = BeautifulSoup(r.text, 'html.parser')
    try:
        table = soup.find('table', class_='table_f')
        trs = table.find_all('tr')
    except AttributeError:
        print('no data for', data)
        return

    rows = trs[3:]
    data_all = {}
    for row in rows:
        ths = row.find_all('th')
        table_cells = [th.text.strip() for th in ths]
        tds = row.find_all('td')
        cells = [td.text.strip() for td in tds]

        if table_cells[0] == '期貨小計':
            break
        if len(table_cells) == 3:
            product = table_cells[1]
            product_who = table_cells[2]
            data = [product, product_who] + cells
        else:
            product_who = table_cells[0]
            data = [product, product_who] + cells

        converted = [int(d.replace(',', '')) for d in data[2:]]
        data = data[:2] + converted

        headers = ['商品', '身份別', '交易多方口數', '交易多方金額', '交易空方口數', '交易空方金額', '交易多空淨口數', '交易多空淨額',
                   '未平倉多方口數', '未平倉多方金額', '未平倉空方口數', '未平倉空方金額', '未平倉淨口數', '未平倉多空淨額']

        product = data[0]
        who = data[1]
        contents = {headers[i]: data[i] for i in range(2, len(headers))}
        if product not in data_all:
            data_all[product] = {who: contents}
        else:
            data_all[product][who] = contents

    with open(f'./futures_data/{Day}.json', 'w') as f:
        json.dump(data_all, f, ensure_ascii=False, indent=4)
        print('saved file to', Day)

threads = []
for i in range(os.cpu_count()):
  threads.append(threading.Thread(target = crawl, args = (i,)))
  threads[i].start()

date = datetime.today()- timedelta(days=1)
while True:
    data = crawl(date)
    date = date - timedelta(days=1)
    if date < datetime.today() - timedelta(days=100):
        break

end = time.time()
print(f'爬取資料總花費時間:{int(end - start)}秒')

for i in range(os.cpu_count()):
  threads[i].join()


