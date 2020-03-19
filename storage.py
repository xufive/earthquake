#!/usr/bin/env python
# coding:utf-8

from datetime import datetime
from elasticsearch import Elasticsearch, client

def create_index():
    """创建索引"""
    
    es = Elasticsearch()
    ic = client.IndicesClient(es)
    
    # 如果索引存在则删除
    try:
        ic.delete(index="earthquake")
    except:
        pass

    # 创建索引
    ic.create(
        index="earthquake", 
        body={
            "mappings": {
                "properties": {
                    "time":     {"type": "date"}, # 发震时间
                    "level":    {"type": "float"}, # 震级 
                    "geo":      {"type": "geo_point"}, # 地理位置
                    "deep":     {"type": "float"}, # 深度 
                    "location": {"type": "text"}, # 位置 
                    "source":   {"type": "keyword"} # 数据来源 
                }
            }
        }
    )

def insert_doc(csv_file, source):
    """数据入库"""

    with open(csv_file, "r", encoding="utf-8") as fp:
        lines = fp.readlines()
    
    total = len(lines)-1 # 文件内数据总量
    success, failure = 0, 0 # 累计成功和失败数量
    section = 10000 # 分批插入，每批次数量
    
    rank = list(range(1, len(lines), section))
    rank.append(len(lines))
    for i in range(len(rank)-1):
        print(rank[i], rank[i+1])
        docs = []
        fail = 0 # 本批次失败数量
        for line in lines[rank[i]:rank[i+1]]:
            data = line.split(",")
            try:
               dt = datetime.strptime(data[0], "%Y-%m-%d %H:%M:%S").isoformat()
            except:
                try:
                    d, t = data[0].split()
                    yy, mm, dd = d.split('-')
                    if mm == '00':
                        mm = '01'
                    if dd == '00':
                        dd = '01'
                    
                    #print("Data Clearing:", data[0])
                    data[0] = '%s-%s-%s %s'%(yy, mm, dd, t)
                    dt = datetime.strptime(data[0], "%Y-%m-%d %H:%M:%S").isoformat()
                except:
                    print("Error:", i, data[0])
                    fail += 1
                    continue
            
            try:
                cmd = {
                    "index":{"_index":"earthquake"}
                }

                doc = {
                    "time": dt,
                    "level": float(data[1]),
                    "geo": [float(data[2]), float(data[3])],
                    "deep": float(data[4]),
                    "location": data[5],
                    "source": source
                }
                docs.append(cmd)
                docs.append(doc)
            except:
                print("Error:", line)
                fail += 1
        
        es = Elasticsearch()
        ret = es.bulk(index='earthquake', body=docs)
        success += len(docs)/2 - fail
        failure += fail
        print("%s共计%d条数据，累计入库%d条，累计失败%d条"%(csv_file, total, success, failure))

if __name__ == "__main__":
    create_index()
    insert_doc("earthQuake_china.csv", 'CEA')
    insert_doc("earthQuake_usa.csv", 'USGS')