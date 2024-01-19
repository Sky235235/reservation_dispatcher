import pandas as pd
import json
from edamodule.QueryConfig import ServiceQuery
from edamodule.DBConfig import DBConfig
from edamodule.InsertLoadModule import DataLoad, UpdateDB, InsertDB
from edamodule.SectorUtils import GetCoordinate
import math
from tqdm import tqdm
import requests

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

    # idx = 221540 ## main.py 함수에 들어갈때는 제거
    print('imminent_reservation_start')
    diff_start = 20 * 60
    diff_end = 1 * 60 * 60
    config_query = ServiceQuery()
    ## 해당 예약 데이터 불러오기
    target_query = config_query.target_reservation(idx)
    target_df = load_data(db_info_data, target_query)
    _target_datetime = list(target_df['target_datetime'])[0]
    _target_unix_time = list(target_df['target_unix_time'])[0]
    _target_lng_list = list(target_df['target_lng'])
    _target_lat_list = list(target_df['target_lat'])
    target_lng = list(target_df['target_lng'])[0]
    target_lat = list(target_df['target_lat'])[0]

    ### car_current 데이터 불러오기
    car_current_query = config_query.airport_reservation_imminent(idx)
    current_df = load_data(db_info_data, car_current_query)
    _current_lng_lst = list(current_df['position_lng'])
    _current_lat_lst = list(current_df['position_lat'])

    ## 카텍 변경
    coordi = GetCoordinate()
    target_converted = coordi.wgstokatec(_target_lng_list, _target_lat_list)
    current_df['target_xpos'] = target_converted[0][0]
    current_df['target_ypos'] = target_converted[1][0]

    ## 직선 거리 구하기
    print('get_direct_dist')
    dist_lst = []
    for i in tqdm(range(len(current_df))):
        x_1 = current_df['position_xpos'][i]
        x_2 = current_df['target_xpos'][i]
        y_1 = current_df['position_ypos'][i]
        y_2 = current_df['target_ypos'][i]

        a = (x_2 - x_1)
        b = (y_2 - y_1)
        _dist = get_dist(a, b)
        dist_lst.append(_dist)

    current_df['direct_dist_list'] = dist_lst
    closed_current_df = current_df.loc[current_df['direct_dist_list'] <= 5000].reset_index(drop=True)
    print('original_df', len(current_df))
    print('less then 5000m df', len(closed_current_df))
    # closed_current_df.to_csv('current_df.csv', encoding='utf-8-sig')

    # ## 예상 도착 일시 구하기
    print('Get estimated_time')

    closed_current_df['target_lng'] = target_lng
    closed_current_df['target_lat'] = target_lat

    base_url = "*********************"
    _duration_lst = []
    for i in tqdm(range(len(closed_current_df))):
        _position_lat = closed_current_df['position_lat'][i]
        _position_lng = closed_current_df['position_lng'][i]
        _target_lat = closed_current_df['target_lat'][i]
        _target_lng = closed_current_df['target_lng'][i]

        params = {'arrivalLat': _target_lat,
                  'arrivalLng': _target_lng,
                  'departureLat': _position_lat,
                  'departureLng': _position_lng,
                  'option': 1}

        res = requests.get(base_url, params=params)
        _result = res.json()
        _duration = _result['durationSecond']
        _duration_lst.append(_duration)

    closed_current_df['durationSecond'] = _duration_lst
    closed_current_df['estimated_arrival_unix_time'] = closed_current_df['position_unix_time'] + closed_current_df['durationSecond']

    ### 예약 출발일시와 예상 도칙시간 차이
    closed_current_df['target_unix_time'] = _target_unix_time
    closed_current_df['time_diff'] = closed_current_df['target_unix_time'] - closed_current_df['estimated_arrival_unix_time']

    final_current_df = closed_current_df[(closed_current_df['time_diff'] >= diff_start) & (closed_current_df['time_diff'] <= diff_end)]
    final_current_df = final_current_df.sort_values(by='time_diff', ascending=True).reset_index(drop=True)

    driver_idx_lst = list(final_current_df['driver_idx'])
    time_diff_lst = list(final_current_df['time_diff'])
    total_list = []
    total_dict = {}
    total_dict['total_car_count'] = len(final_current_df)
    total_list.append(total_dict)
    for i in range(len(final_current_df)):
        _dict = {}
        _dict['driver_idx'] = driver_idx_lst[i]
        _dict['time_diff_second'] = time_diff_lst[i]
        total_list.append(_dict)

    return total_list
