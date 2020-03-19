#!/usr/bin/env python
# coding:utf-8

from pyecharts import options as opts
from pyecharts.charts import Geo
from pyecharts.globals import ChartType
from datetime import datetime
from elasticsearch import Elasticsearch
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['FangSong'] # 指定默认字体
plt.rcParams['axes.unicode_minus'] = False     # 解决保存图像时'-'显示为方块的问题

def get_data(level, year, source):
    '''获取地震等级不小于level的地震数据'''

    es = Elasticsearch()
    dt = datetime.strptime(str(year), '%Y').isoformat()
    condition = {
        'size': 0,
        'track_total_hits': True,
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'level': {
                                'gte': level
                            }
                        }
                    },
                    {
                        'range': {
                            'time': {
                                'gt': dt
                            }
                        },
                    }
                ]
            }
        },
        'aggregations': {
            'heatmap': {
                'geohash_grid': {
                    'field': 'geo',
                    'precision': 5
                },
                'aggs': {
                    'centroid': { 
                        'geo_centroid': {
                            'field': 'geo'
                        }
                    }
                }
            }
        }
    }
    
    if source == 'CEA' or source == 'USGS':
        condition['query']['bool']['must'].append({'term':{'source':source}})
    
    return es.search(index='earthquake', body=condition)

def plot_heatmap(level, maptype, source, year=1900, cb=(0,10)):
    '''绘制地震热力图
    
    level   - 仅绘制不小于level等级的地震数据
    maptype - 地图类型：china|world
    source  - 数据源：CEA|USGS
    year    - year：起始年份
    cb      - ColorBar显示的最小值和最大值
    '''
    
    zone = '中国' if maptype == 'china' else '全球'
    subject = '公元%d年至今%s%d级以上地震热力图（%s）'%(year, zone, level, source)
    data = get_data(level, year, source)
    #print(data['hits']['total']['value'])
    
    c = Geo(init_opts={'width':'1700px', 'height':'800px'})
    c.add_schema(maptype=maptype)
    values = []
    for bucket in data['aggregations']['heatmap']['buckets']:
        c.add_coordinate(bucket['key'], bucket['centroid']['location']['lon'], bucket['centroid']['location']['lat'])
        values.append((bucket['key'], bucket['doc_count']))

    c.add(subject, values, type_=ChartType.HEATMAP)
    c.set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    c.set_global_opts(
        visualmap_opts=opts.VisualMapOpts(min_=cb[0], max_=cb[1], is_calculable=True, orient='horizontal', pos_left='center'),
        title_opts=opts.TitleOpts(title='Geo-HeatMap'),
    )
    c.render('%s.html'%subject)
    
def top_10(year, source):
    """条件检索"""
    
    dt = datetime.strptime(str(year), '%Y').isoformat()
    condition = {
        'size': 10, 
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'time': {
                                'gt': dt
                            }
                        },
                    },
                    {
                        'term': {
                            'source': source
                        }
                    }
                ]
            }
        }, 
        'sort': {
            'level': {
                'order': 'desc'
            }
        },
        'highlight': {
            'fields': {
                'time': {},
                'level': {},
                'location': {}
            }
        }
    }
    
    es = Elasticsearch()
    ret = es.search(index='earthquake', body=condition)
    
    result = list()
    for item in ret['hits']['hits']:
        result.append((item['_source']['time'], item['_source']['level'], item['_source']['location'].strip()))
    
    return result
    
def search_by_condition(location, level, year=1900, source='CEA', size=200):
    """条件检索"""
    
    dt = datetime.strptime(str(year), '%Y').isoformat()
    condition = {
        'size': size, 
        'query': {
            'bool': {
                'must': [
                    {
                        'range': {
                            'level': {
                                'gte': level
                            }
                        }
                    },
                    {
                        'range': {
                            'time': {
                                'gt': dt
                            }
                        },
                    },
                    {
                        'match_phrase': {
                            'location': {
                                'query': location,
                                'slop': 0
                            }
                        }
                    },
                    {
                        'term': {
                            'source': source
                        }
                    }
                ]
            }
        }, 
        'sort': {
            'time': {
                'order': 'desc'
            }
        },
        'highlight': {
            'fields': {
                'time': {},
                'level': {},
                'location': {}
            }
        }
    }
    
    es = Elasticsearch()
    ret = es.search(index='earthquake', body=condition)
    
    result = list()
    for item in ret['hits']['hits']:
        result.append((item['_source']['time'], item['_source']['level'], item['_source']['location'].strip()))
    
    return result
    
def plot_bar(city_list, level_list, year=1900, source='CEA', size=200):
    """绘制城市分级地震柱状图"""
    
    title = '公元%d年至今中国部分省区地震次数柱状图（%s）'%(year, source)
    fig, ax = plt.subplots()
    fig.set_size_inches(12, 6)
    
    for level in level_list:
        data = list()
        for city in city_list:
            data.append(len(search_by_condition(city, level, year=year, source=source, size=size)))
        #print(level, data)
        ax.bar(city_list, data, 0.35, label='%d级及以上'%level)
    
    ax.legend(loc='upper left')
    ax.set_ylabel('地震次数')
    ax.set_title(title)
    fig.savefig('%s.png'%title)

if __name__ == '__main__':
    # 公元1000年至今全球5级以上地震热力图（CEA）
    plot_heatmap(5, 'world', source='CEA', year=1000, cb=(0,10))
    
    # 公元1900年至今全球7级以上地震热力图（USGS）
    plot_heatmap(7, 'world', source='USGS', year=1900, cb=(0,5))
    
    # 公元1900年至今全球7级以上地震热力图（CEA）
    plot_heatmap(7, 'world', source='CEA', year=1900, cb=(0,5))
    
    # 公元1000年至今中国5级以上地震热力图（CEA）
    plot_heatmap(5, 'china', source='CEA', year=1000, cb=(0,20))
    
    # 公元1000年至今中国7级以上地震热力图（CEA）
    plot_heatmap(7, 'china', source='CEA', year=1000, cb=(0,3))
    
    # 公元1900年至今中国7级以上地震热力图（CEA）
    plot_heatmap(7, 'china', source='CEA', year=1900, cb=(0,1))
    
    # 公元1900年至今中国部分省区地震次数柱状图（CEA）
    city_list = ['北京', '上海', '广东', '江苏', '浙江', '山东', '台湾', '河南', '安徽', '云南', '贵州', '四川', '湖北', '陕西', '新疆', '河北', '甘肃', '江西', '吉林', '辽宁']
    level_list = [6, 7]
    plot_bar(city_list, level_list, year=1900, source='CEA', size=2000)
    
    # 最强地震TOP10
    for year, source in [(1900, 'USGS'), (1900, 'CEA'), (1000, 'CEA')]:
        top = top_10(year=year, source=source)
        print('自公元%d年以来最强地震TOP10（%s）'%(year, source))
        for i, item in enumerate(top):
            print('|%d|%s|%.1f|%s|'%((i+1), *item))
        print('----------------------------------')
    
    # 公元1000年以来济南地震史
    print('公元1000年以来济南地震史：')
    res = search_by_condition('济南', 0, 1000)
    for i, item in enumerate(res):
        print('%d. %s %.1f %s'%((i+1), *item))
    
    