import requests
import pandas as pd
import time
import json
from datetime import date
from dateutil.relativedelta import relativedelta
import os
import glob # 임시 파일들을 찾기 위해 추가

# =================================================================
# ⚠️ 1. 필수 설정 값 (API 승인 후 여기에 입력하세요!)
# =================================================================

# 🔑 공공데이터 포털에서 발급받은 인증키 (실제 키로 교체 필요)
SERVICE_KEY = "Cq480HPo0y5QFitQIm+UL3uFOjcVKGpcaDbZi9BX7EG58UgzgC+FSDerU8zGtdNzZSh+fKIIL354yDRejTw+vA=="

# 🏙️ 수도권 주요 ASOS 관측 지점 번호 (Stn ID)
STN_IDS = ['108', '112', '119', '203', '99', '201'] 

# 🗓️ 조회 기간 설정
START_YEAR = 2021
END_YEAR = 2024

# 📁 파일 경로 설정
DATA_DIR = 'data'
TEMP_FILE_PREFIX = 'temp_asos_chunk_'
FINAL_OUTPUT_FILE = os.path.join(DATA_DIR, 'asos_su-do-gwon_final.csv')

# =================================================================
# 2. API 설정 및 데이터 수집 함수
# =================================================================

BASE_URL = "http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList"

COMMON_PARAMS = {
    'serviceKey': SERVICE_KEY,
    'dataCd': 'ASOS',
    'dateCd': 'HR',
    'dataType': 'JSON',
    'endHh': '23',
    'startHh': '00',
    'numOfRows': '50', # 504 Time-out 방지를 위해 요청 건수를 50으로 낮춤
}

def fetch_asos_data(stn_id, start_date_str, end_date_str):
    """특정 지점과 기간의 ASOS 시간 자료를 API로 수집합니다. (Time-out 시 자동 재시도 포함)"""
    
    params = COMMON_PARAMS.copy()
    params.update({
        'stnIds': stn_id,
        'startDt': start_date_str,
        'endDt': end_date_str,
    })
    
    all_records = []
    page_no = 1
    MAX_RETRIES = 3 # 최대 재시도 횟수
    
    while True:
        params['pageNo'] = str(page_no)
        response = None
        
        # --- Time-out 및 Status Code 오류 시 재시도 루프 ---
        for attempt in range(MAX_RETRIES):
            try:
                # Time-out 90초 설정
                response = requests.get(BASE_URL, params=params, timeout=90)
                
                if response.status_code != 200:
                    print(f"[{stn_id}, {start_date_str}, p{page_no}] 상태 코드 {response.status_code}. 재시도 ({attempt + 1}/{MAX_RETRIES})")
                    time.sleep(2 ** attempt * 2) 
                    continue
                
                break 
            except requests.exceptions.Timeout:
                print(f"[{stn_id}, {start_date_str}, p{page_no}] 요청 시간 초과(Timeout). 재시도 ({attempt + 1}/{MAX_RETRIES})")
                time.sleep(2 ** attempt * 3) 
                continue
            except Exception as e:
                print(f"[FATAL ERROR] 예기치 않은 연결 오류 발생: {e}")
                return all_records
        
        if response is None or response.status_code != 200:
            return all_records

        # --- 데이터 처리 ---
        try:
            data = response.json()
            response_body = data.get('response', {}).get('body')
            
            if not response_body:
                header_msg = data.get('response', {}).get('header', {}).get('resultMsg', 'Unknown Error')
                print(f"[FAIL] 데이터 없음 또는 오류: {header_msg}")
                break
                
            items = response_body.get('items', {}).get('item')
            
            if not items:
                break
                
            all_records.extend(items)
            total_count = response_body.get('totalCount', 0)
            
            if len(all_records) >= total_count:
                break
                
            page_no += 1
            time.sleep(0.5) 
            
        except json.JSONDecodeError:
            print(f"[FAIL] JSON 디코딩 오류 (응답 본문 확인 필요).")
            break
        except Exception as e:
            print(f"[FATAL ERROR] 데이터 처리 중 오류: {e}")
            break

    return all_records

# =================================================================
# 3. 메인 실행 및 저장 (분기별 반복 및 임시 저장)
# =================================================================

if SERVICE_KEY == "YOUR_SERVICE_KEY_HERE":
    print("🔴 실행 실패: SERVICE_KEY를 발급받은 실제 키로 교체 후 다시 실행하세요.")
else:
    # data 폴더 존재 확인 및 생성
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    current_date = date(START_YEAR, 1, 1)
    end_date_limit = date(END_YEAR + 1, 1, 1) # 다음 해 1월 1일 직전까지

    print("--- 기상청 ASOS 데이터 수집 시작 (분기별 분할 요청) ---")

    while current_date < end_date_limit:
        next_quarter = current_date + relativedelta(months=3)
        
        start_dt_str = current_date.strftime("%Y%m%d")
        # 종료일은 다음 분기의 전날로 설정
        end_dt_str = (next_quarter - relativedelta(days=1)).strftime("%Y%m%d")

        # 임시 파일명 정의
        temp_file_name = f"{TEMP_FILE_PREFIX}{start_dt_str}_{end_dt_str}.csv"
        temp_file_path = os.path.join(DATA_DIR, temp_file_name)
        
        # 해당 분기의 모든 데이터를 저장할 리스트 (메모리 절약을 위해 임시 파일 저장 후 초기화됨)
        quarter_records = []

        print(f"\n[분기 시작] {start_dt_str} ~ {end_dt_str}")

        # --- 분기별 API 호출 루프 ---
        for stn_id in STN_IDS:
            print(f"-> 수집 중: 지점 {stn_id}")
            records = fetch_asos_data(stn_id, start_dt_str, end_dt_str)
            
            if records:
                quarter_records.extend(records)
                print(f"   [SUCCESS] {len(records):,} 건 수집 완료.")
            else:
                print(f"   [FAIL] 데이터 수집 실패 또는 데이터 없음.")
            
            time.sleep(1) # API 호출 제한 방지를 위한 안전 대기 시간
        # ----------------------------
        
        if quarter_records:
            # --- 분기별 데이터 임시 파일로 저장 ---
            df_quarter = pd.DataFrame(quarter_records)
            
            # 파일이 이미 존재하는 경우 (중단 후 재시작), 헤더 없이 이어붙임
            write_header = not os.path.exists(temp_file_path)
            
            df_quarter.to_csv(temp_file_path, index=False, encoding='utf-8-sig', header=write_header)
            
            print(f"\n💾 분기 데이터 임시 저장 완료: {temp_file_path} (총 {len(quarter_records):,} 건)")
        
        # 다음 분기로 이동
        current_date = next_quarter

    # =================================================================
    # 4. 최종 파일 합본 생성 및 임시 파일 삭제
    # =================================================================
    print("\n\n---  최종 파일 합본 생성 시작 ---")
    
    all_temp_files = glob.glob(os.path.join(DATA_DIR, f'{TEMP_FILE_PREFIX}*.csv'))
    
    if all_temp_files:
        combined_df = []
        
        # 첫 번째 파일만 헤더를 포함하고 나머지는 데이터만 읽음
        for i, file_path in enumerate(all_temp_files):
            df_chunk = pd.read_csv(file_path, encoding='utf-8-sig')
            combined_df.append(df_chunk)

        df_final_weather = pd.concat(combined_df, ignore_index=True)
        
        # 최종 파일 저장
        df_final_weather.to_csv(FINAL_OUTPUT_FILE, index=False, encoding='utf-8-sig')
        
        # 임시 파일 삭제
        #for file_path in all_temp_files:
             #os.remove(file_path)
        
        print("\n=============================================")
        print(f"✅ 최종 기상 데이터 합본 완료! 총 {len(df_final_weather):,} 건.")
        print(f"💾 파일 저장 경로: {FINAL_OUTPUT_FILE}")
        print("🗑️ 임시 파일 모두 삭제 완료.")
        print("=============================================")
    else:
        print("\n🔴 최종적으로 수집된 데이터가 없습니다. 설정을 확인하세요.")