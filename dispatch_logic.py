import pandas as pd
import json
from edamodule.QueryConfig import ServiceQuery
from edamodule.DBConfig import DBConfig
from edamodule.InsertLoadModule import DataLoad, UpdateDB, InsertDB
from edamodule.SectorUtils import GetCoordinate
import math
from tqdm import tqdm
import requests
from datetime import datetime, timedelta


### 데이터 로드 통합 함수
with open('edamodule/dbconfiginfo.json') as f:
    db_info_data = json.load(f)

def load_data(db_info_data, query):
    _dbconfig = DBConfig(db_info_data)
    _conn, _curs = _dbconfig.ServiceRO()
    _loadconfig = DataLoad(_conn, _curs)
    data = _loadconfig.get_data(query)
    del _loadconfig
    return data

## 거리 계산
def get_dist(a, b):
    dist = math.sqrt((a**2) + (b**2))

    return dist
def run(idx):
    print('general_reservation_start')
    # idx = 221752 ## main.py 함수에 들어갈때는 제거
    diff_start = 30 * 60 ## 15분
    diff_end = 2 * 60 * 60 ## 9시간
    config_query = ServiceQuery()
    ## 해당 예약 데이터 불러오기
    target_query = config_query.target_reservation(idx)
    target_df = load_data(db_info_data, target_query)
    _target_datetime = list(target_df['target_datetime'])[0]
    _target_unix_time = list(target_df['target_unix_time'])[0]
    _target_lng_list = list(target_df['target_lng'])
    _target_lat_list = list(target_df['target_lat'])
    _target_lng = list(target_df['target_lng'])[0]
    _target_lat = list(target_df['target_lat'])[0]
    ##예약리스트 데이터 불러오기
    res_list_query = config_query.airport_general_reservation(idx)
    res_df = load_data(db_info_data, res_list_query)
    _res_arrival_lng_lst = list(res_df['arrival_lng'])
    _res_arrival_lat_lst = list(res_df['arrival_lat'])

    ## 카텍 변경
    coordi = GetCoordinate()
    target_converted = coordi.wgstokatec(_target_lng_list, _target_lat_list)
    res_converted = coordi.wgstokatec(_res_arrival_lng_lst, _res_arrival_lat_lst)
    res_df['target_xpos'] = target_converted[0][0]
    res_df['target_ypos'] = target_converted[1][0]
    res_df['arrival_xpos'] = res_converted[0]
    res_df['arrival_ypos'] = res_converted[1]
    print('around_data', len(res_df))

    ## 직선 거리 구하기
    print('get_direct_dist')
    dist_lst = []
    for i in tqdm(range(len(res_df))):

        x_1 = res_df['arrival_xpos'][i]
        x_2 = res_df['target_xpos'][i]
        y_1 = res_df['arrival_ypos'][i]
        y_2 = res_df['target_ypos'][i]

        a = (x_2-x_1)
        b = (y_2-y_1)
        _dist = get_dist(a, b)
        dist_lst.append(_dist)

    res_df['direct_dist_list'] = dist_lst
    closed_current_df = res_df.loc[res_df['direct_dist_list'] <= 5000].reset_index(drop=True)
    print('less then 5000m df', len(closed_current_df))

    ## 예상 도착 일시 구하기
    print('Get estimated_time')
    base_url = "****************"
    _duration_lst = []
    for i in tqdm(range(len(closed_current_df))):
        _arrival_lat = closed_current_df['arrival_lat'][i]
        _arrival_lng = closed_current_df['arrival_lng'][i]
        _departure_lat = closed_current_df['departure_lat'][i]
        _departure_lng = closed_current_df['departure_lng'][i]

        params = {'arrivalLat': _arrival_lat,
                  'arrivalLng': _arrival_lng,
                  'departureLat': _departure_lat,
                  'departureLng': _departure_lng,
                  'option': 1}

        res = requests.get(base_url, params=params)
        _result = res.json()
        _duration = _result['durationSecond']
        _duration_lst.append(_duration)

    closed_current_df['durationSecond'] = _duration_lst
    closed_current_df['estimated_arrival_unix_time'] = closed_current_df['reservation_unix_time'] + closed_current_df['durationSecond']
    closed_current_df['target_unix_time'] = _target_unix_time
    closed_current_df['time_diff'] = closed_current_df['target_unix_time'] - closed_current_df['estimated_arrival_unix_time']

    final_res_df = closed_current_df[(closed_current_df['time_diff'] >= diff_start) & (closed_current_df['time_diff'] <= diff_end)]
    final_res_df = final_res_df.sort_values(by='time_diff', ascending=True).reset_index(drop=True)

    driver_idx_lst = list(final_res_df['driver_idx'])
    time_diff_lst = list(final_res_df['time_diff'])
    total_list = []
    total_dict = {}
    total_dict['total_car_count'] = len(final_res_df)
    total_list.append(total_dict)
    for i in range(len(final_res_df)):
        _dict = {}
        _dict['driver_idx'] = driver_idx_lst[i]
        _dict['time_diff_second'] = time_diff_lst[i]
        total_list.append(_dict)
    return total_list