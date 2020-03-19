#!/usr/bin/env python
# coding:utf-8

import time
import re
import requests
from bs4 import BeautifulSoup

def Crawl_data(url, csv_file):
    '''抓取地震数据'''
    
    # 获取总页数
    resp = requests.get(url)
    r = re.compile('查询到 (\d+) 条记录，分 (\d+) 页显示')
    pcount = int(r.findall(resp.text)[0][1])

    # 取得每页数据表格，并写到csv文件中
    with open(csv_file, 'w', encoding='utf-8') as fp:
        fp.write('发震时刻, 震级(M), 经度(°), 纬度(°), 深度(千米), 参考位置, \n')
        for page in range(1, pcount+1):
        #for page in range(2):
            print('第%d页/共%d页' % (page, pcount), '...', end='')
            try_count = 0
            resp = requests.get(url+'&page=%d'%page)
            while not resp.ok and try_count < 2:
                try_count += 1
                time.sleep(try_count*1)
                resp = requests.get(url+'&page=%d'%page)
            
            if not resp.ok:
                print('Error:', url+'&page=%d'%page)
                continue
            
            soup = BeautifulSoup(resp.text, 'lxml')
            for tr in soup.find_all('tr')[1:]:
                tds = tr.find_all('td')
                dt = tds[0].text
                level = tds[1].text
                lon = tds[2].text
                lat = tds[3].text
                deep = tds[4].text
                location = tds[5].find('a').text
                fp.write('%s, %s, %s, %s, %s, %s\n'%(dt, level, lon, lat, deep, location))
            print('Done')

if __name__ == '__main__':
    Crawl_data('http://ditu.92cha.com/dizhen.php?dizhen_ly=china', 'earthQuake_china.csv')
    Crawl_data('http://ditu.92cha.com/dizhen.php?dizhen_ly=usa', 'earthQuake_usa.csv')
    