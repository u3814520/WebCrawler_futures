# from concurrent.futures import as_completed , ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import os
import time

if not os.path.exists('./futures_data'):
    os.mkdir('./futures_data')

start = time.time()


def crawl(date):
    print('crawling' + ' ' + date.strftime('%Y%m%d'))
    r = requests.get(
        'https://www.taifex.com.tw/cht/3/futContractsDate?queryDate={}%2F{}%2F{}'.format(date.year, date.month,
                                                                                         date.day))
    soup = BeautifulSoup(r.text, 'html.parser')
    try:
        table = soup.find('table', class_='table_f')
        trs = table.find_all('tr')
    except AttributeError:
        print('no data for', date)
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
    return data_all,date

def save_json(date, data, path):
    file = os.path.join(path, 'futures' + date.strftime('%Y%m%d') + '.json')
    with open(file, 'w') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print('saved file to', file)

def main():
    download_dir = 'futures_data'
    os.makedirs(download_dir, exist_ok=True)

    start = time.time()

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        today = datetime.today()
        date = today

        while True:
            future = executor.submit(crawl, date)
            futures.append(future)

            date = date - timedelta(days=1)
            if date <= today - timedelta(days=1096):
                break

        for future in as_completed(futures):
            if future.result():
                date, data_all = future.result()
                save_json(data_all, date, download_dir)

    end = time.time()
    print(f'下載資料共花費{int(end - start)}秒')


if __name__ == '__main__':
    main()


