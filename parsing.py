# -*- coding: utf-8 -*-
"""
Created on Sat Feb 22 11:22:43 2020

@author: Dick
"""

import urllib
# import re
import time
import sys
import os
import datetime 
# from urllib.request import urlopen
from urllib.error import HTTPError, URLError
from collections import defaultdict

#from bs4 import BeautifulSoup
import pymongo
from pyquery import PyQuery as pq

def log_print(*args, **kwargs):
    print(*args, file=sys.stdout, **kwargs)    



class CathayWebScraping(object):    
    
    def __init__(self, sleep_second, header, parsing_region, total_rows=None):
        self.sleep_second = sleep_second
        self.header = header
        self.parsing_region = parsing_region
        self.total_rows = total_rows
        self.opener = urllib.request.build_opener()
        self.opener.addheaders = [self.header]


    def __chunk_for_rent_ids(func):
        """
        Cut down the length of input rent id list to these two methods to prevent from receiving
        http error : request too large. Default chunk size is 300.
        """
        def inner(self, rent_id_list, chunk_size=300):
            _generator = (rent_id_list[i: i + chunk_size]
                          for i in range(0, len(rent_id_list), chunk_size))
            detail_dict = defaultdict(dict)
            for chunked_rent_id_list in _generator:
                detail_dict_update = func(self, chunked_rent_id_list)
                for k, v in detail_dict_update.items():
                    detail_dict[k] = v
                self.storing_to_mongodb(detail_dict_update)
            return detail_dict
        return inner        
        
    def storing_to_mongodb(self, detail_dict=None):
        if detail_dict is None:
            detail_dict = self.parsing_591_details()
        _mongodb_con_str = os.environ.get('mongodb_con_str')
        client = pymongo.MongoClient(_mongodb_con_str)
        db = client.cathay_parsing
        try:
            for key, _rent in detail_dict.items():
                db.rent.update_one({"rent_id": key},
                                     {"$set":{"region": _rent["region"],
                                              'userInfo': _rent["userInfo"],
                                              'kfCallName': _rent["kfCallName"],
                                              'dialPhoneNum': _rent["dialPhoneNum"],
                                              'type': _rent["type"],
                                              'status': _rent["status"],
                                              'genderRestrict': _rent["genderRestrict"],
                                              'updated': datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}, 
                                      "$setOnInsert": {"created": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}}, # 如果沒紀錄的話就 add created
                                 upsert=True)                
#                db.rent_2.insert_one({"rent_id": key,
#                                    "region": _rent["region"],
#                                    'userInfo': _rent["userInfo"],
#                                    'kfCallName': _rent["kfCallName"],
#                                    'dialPhoneNum': _rent["dialPhoneNum"],
#                                    'type': _rent["type"],
#                                    'status': _rent["status"],
#                                    'genderRestrict': _rent["genderRestrict"]})
        except Exception as e:
            log_print("{}".format(e))
            
    @__chunk_for_rent_ids    
    def parsing_591_details(self, rent_detail_list=None):
        if rent_detail_list is None:
            rent_detail_list = self.parsing_591_links()
        detail_dict = defaultdict(dict)
        for index, _detail_href in enumerate(rent_detail_list):
            # https://rent.591.com.tw/rent-detail-8753781.html
            try:
                _concat_href = "https://rent.591.com.tw/rent-detail-"+ _detail_href[1:] + ".html"
                detail_text = self.opener.open(_concat_href)
            except HTTPError as e:
                log_print("{}: {}".format(e, _detail_href))
                continue
            except URLError as e:
                log_print("{}: {}".format(e, _detail_href))
                continue
            else:
                if index % 100 == 0:
                    log_print('ok: {} / {}'.format(index, _concat_href))
            
            try:
                doctree_detail = pq(detail_text.read())
            except Exception as e:
                log_print("{}: {}".format(e, _detail_href))
                detail_dict[_detail_href[1:]] = {"region": "",
                                                 "userInfo": "",
                                                 "kfCallName": "",
                                                 "dialPhoneNum": "",
                                                 "type": "",
                                                 "status": "",
                                                 "genderRestrict": "",
                                                }
                continue 
                
            try:
                #_userInfo = doctree_detail('.userInfo > div.infoOne.clearfix').text().split("\n")[0]
                _region = doctree_detail('#propNav a').eq(2).text()
                _userInfo = doctree_detail('div[class="avatarRight"] > div:first').text() # 改善亂碼情形(eg. 8689219)
                _kfCallName = doctree_detail('.kfCallName').attr('data-name')
                _dialPhoneNum = doctree_detail('.dialPhoneNum').attr('data-value')
                
                if len(_dialPhoneNum) == 0:
                    _dialPhoneNum = doctree_detail('#hid_tel').attr("value") or doctree_detail('.hidtel').text()
                    
                _type = ''.join([i if ord(i) > 256 else '' for i in doctree_detail('li:contains("型")').text()])
                _status = ''.join([i if ord(i) > 256 else '' for i in doctree_detail('li:contains("現況")').text()])
                _genderRestrict = "".join([i.siblings()('em').text() for i in doctree_detail('.clearfix .one').items() if "性別" in i.text()])
            except Exception as e:
                log_print("{}: {}".format(e, _concat_href))        
            
            detail_dict[_detail_href[1:]] = {"region": _region,
                                             "userInfo": _userInfo,
                                             "kfCallName": _kfCallName,
                                             "dialPhoneNum": _dialPhoneNum,
                                             "type": _type,
                                             "status": _status,
                                             "genderRestrict": _genderRestrict,
                                             }
            time.sleep(1)
        return detail_dict
    
    def parsing_591_links(self, first_row=0):
        rent_detail_list = list()
        try:
            region_dict = {"台北市": 1,
                           "新北市": 3}
            region = region_dict.get(self.parsing_region, None)
        except KeyError:
            log_print("Input parsing_region does not support now.Try input 台北市 or 新北市")
        
        url = "https://m.591.com.tw/mobile-list.html?type=rent&regionid={}&firstRow={}".format(region, first_row)
        if self.total_rows is None:
            _t =  self.opener.open(url)
            _doc = pq(_t.read())
            try:
                self.total_rows = int(_doc('#totalPage').attr('value')) or int(_doc('#house_data_arr').attr('value'))
            except ValueError:
                log_print("fetch_totalrows_failed")
                self.total_rows = 30000
        print(self.total_rows)
                                      
        for i in range(first_row, self.total_rows, 8):
            try:
                text = self.opener.open("https://m.591.com.tw/mobile-list.html?type=rent&regionid={}&firstRow={}".format(region, i))
            except HTTPError as e:
                log_print(e)
            except URLError as e:
                log_print(e)
            else:
                log_print('ok (region:{} , firstRow:{})'.format(region, i))
            doctree = pq(text.read())
            detail_href_list = [ i.attr('data-house') for i in doctree('li[class="data choose-li"]').items() if i.attr('data-house') is not None]
            rent_detail_list.extend(detail_href_list)
            time.sleep(self.sleep_second)
        rent_detail_list = list(set(rent_detail_list))
        return rent_detail_list
    
    
if __name__ == '__main__':
    print('Call it locally')
    _parsing_region = sys.argv[1]
    try:
        _total_rows = int(sys.argv[2])
    except Exception:
        _total_rows = None
    print(_parsing_region, _total_rows)
    _c = CathayWebScraping(sleep_second=1,
                           header=("User-Agent",
                                   "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Mobile Safari/537.36"),
                           parsing_region=_parsing_region,
                           total_rows=_total_rows)
    rent_detail_list = _c.parsing_591_links()
    _c.parsing_591_details(rent_detail_list)