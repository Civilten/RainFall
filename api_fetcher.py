import urllib.request
import json
import pandas as pd
import time
from datetime import datetime
import xml.etree.ElementTree as ET

WAMIS_KEY = "bffb72855f69375f93bcb64cc09c6663b0b4c37f96"
KMA_KEY = "HvokGPRZbeFQUcKSimX3c2wHp%2FutF5DHfJd9KooXFlNeoNN0LyX%2FoxW8tyD4ck4NQxz8QEE%2BDe8Sm7S202DnRw%3D%3D"

def _generate_chunks(start_date: str, end_date: str) -> list:
    """YYYYMMDD 범위를 6개월 단위 청크 리스트로 변환 (문자열 비교 가능)"""
    start_yr, end_yr = int(start_date[:4]), int(end_date[:4])
    chunks = []
    for year in range(start_yr, end_yr + 1):
        for h_start, h_end in [(f"{year}0101", f"{year}0630"), (f"{year}0701", f"{year}1231")]:
            cs = max(h_start, start_date) if year == start_yr else h_start
            ce = min(h_end,   end_date)   if year == end_yr   else h_end
            if cs <= ce:
                chunks.append((cs, ce))
    return chunks

def fetch_wamis_hourly_rainfall(stn_cd, start_year, end_year, start_date=None, end_date=None):
    """
    Fetch hourly rainfall data from WAMIS API for a given station and year range.
    Returns a pandas DataFrame formatted identically to the historical database:
    [STN_CD, YEAR, MONTH, DAY, H1, ..., H24]
    """
    records = []

    if start_date and end_date:
        all_chunks = _generate_chunks(start_date, end_date)
    else:
        all_chunks = []
        for year in range(start_year, end_year + 1):
            all_chunks += [(f"{year}0101", f"{year}0630"), (f"{year}0701", f"{year}1231")]

    for start_dt, end_dt in all_chunks:
            url = f"http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_hrdata?obscd={stn_cd}&startdt={start_dt}&enddt={end_dt}&output=json&key={WAMIS_KEY}"
            
            success = False
            for attempt in range(3):
                try:
                    req = urllib.request.Request(url)
                    with urllib.request.urlopen(req, timeout=15) as response:
                        if response.status == 200:
                            data = json.loads(response.read().decode('utf-8'))
                            
                            if data.get("result", {}).get("code") == "success":
                                items = data.get("list", [])
                                for item in items:
                                    ymdh = item.get("ymdh", "")
                                    rf = item.get("rf", "-")
                                    
                                    if len(ymdh) == 10:
                                        y = int(ymdh[0:4])
                                        m = int(ymdh[4:6])
                                        d = int(ymdh[6:8])
                                        h = int(ymdh[8:10])
                                        
                                        try:
                                            val = float(rf) if rf != "-" and rf != "" else 0.0
                                        except ValueError:
                                            val = 0.0
                                            
                                        records.append({
                                            "STN_CD": stn_cd,
                                            "YEAR": y,
                                            "MONTH": m,
                                            "DAY": d,
                                            "HOUR": h,
                                            "RF": val
                                        })
                                success = True
                                break # Exit retry loop on success
                            else:
                                # API returned 200 but result code is not success (e.g. no data)
                                success = True
                                break
                except Exception as e:
                    print(f"WAMIS API {year} ({start_dt}-{end_dt}) error: {e}")
                    time.sleep(3)
            
            if not success:
               print(f"WAMIS API failed for {year} ({start_dt}-{end_dt}) after 3 attempts.")
            
            time.sleep(1)
            
    if not records:
        return pd.DataFrame()
        
    # Convert list of dicts to flat DataFrame (Hourly columns)
    df_raw = pd.DataFrame(records)
    
    # Pivot to get H1...H24 columns
    # Pandas pivot table: index=[STN_CD, YEAR, MONTH, DAY], columns=HOUR, values=RF
    pivot_df = df_raw.pivot_table(index=['STN_CD', 'YEAR', 'MONTH', 'DAY'], 
                                  columns='HOUR', 
                                  values='RF', 
                                  aggfunc='first').reset_index()
    
    # Ensure all 24 columns exist, fill missing with 0.0
    for h in range(1, 25):
        if h not in pivot_df.columns:
            pivot_df[h] = 0.0
            
    # Rename hour columns to H1...H24
    hour_cols = {h: f"H{h}" for h in range(1, 25)}
    pivot_df = pivot_df.rename(columns=hour_cols)
    
    
    # Rearrange columns exactly like history DB
    final_cols = ['STN_CD', 'YEAR', 'MONTH', 'DAY'] + [f"H{h}" for h in range(1, 25)]
    final_df = pivot_df[final_cols].fillna(0.0)
    
    return final_df

def fetch_kma_hourly_rainfall(stn_cd, start_year, end_year, start_date=None, end_date=None):
    """
    Fetch hourly rainfall data from KMA API for a given station and year range.
    Returns a pandas DataFrame formatted identically to the historical database.
    """
    records = []
    
    # KMA STN_ID is usually the last 3 digits
    # Extract only the last 3 characters, assuming stn_cd is like '10011108' -> '108'
    kma_cd = stn_cd[-3:].lstrip('0') if len(stn_cd) > 3 else stn_cd
    # Usually KMA IDs are like 108, 90 etc. So integer str representation works reliably
    try:
        kma_cd = str(int(stn_cd[-3:]))
    except:
        pass # fallback
        
    from datetime import timedelta

    if start_date and end_date:
        all_chunks = _generate_chunks(start_date, end_date)
    else:
        all_chunks = []
        for year in range(start_year, end_year + 1):
            all_chunks += [(f"{year}0101", f"{year}0630"), (f"{year}0701", f"{year}1231")]

    for start_dt, end_dt in all_chunks:
            for page in range(1, 10): # up to 10 pages, just in case
                url = f"http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList?serviceKey={KMA_KEY}&numOfRows=999&pageNo={page}&dataType=JSON&dataCd=ASOS&dateCd=HR&stnIds={kma_cd}&endDt={end_dt}&endHh=00&startHh=01&startDt={start_dt}"
                
                success = False
                items_fetched = 0
                for attempt in range(3):
                    try:
                        req = urllib.request.Request(url)
                        with urllib.request.urlopen(req, timeout=15) as response:
                            if response.status == 200:
                                data = json.loads(response.read().decode('utf-8'))
                                header = data.get("response", {}).get("header", {})
                                
                                if header.get("resultCode") == "00": # Normal
                                    items_dict = data.get("response", {}).get("body", {}).get("items", {})
                                    if isinstance(items_dict, dict) and "item" in items_dict:
                                        items = items_dict.get("item", [])
                                        items_fetched = len(items)
                                        for item in items:
                                            tm = item.get("tm", "") # e.g. "2018-01-01 01:00"
                                            rn = item.get("rn", "")
                                            
                                            if len(tm) >= 13:
                                                y = int(tm[0:4])
                                                m = int(tm[5:7])
                                                d = int(tm[8:10])
                                                h = int(tm[11:13])
                                                
                                                # KMA midnight is 00:00 of the NEXT day, but we need H24 of CURRENT day
                                                if h == 0:
                                                    dt = datetime(y, m, d) - timedelta(days=1)
                                                    y, m, d, h = dt.year, dt.month, dt.day, 24
                                                    
                                                try:
                                                    val = float(rn) if rn != "" else 0.0
                                                except ValueError:
                                                    val = 0.0
                                                    
                                                records.append({
                                                    "STN_CD": stn_cd,
                                                    "YEAR": y,
                                                    "MONTH": m,
                                                    "DAY": d,
                                                    "HOUR": h,
                                                    "RF": val
                                                })
                                        
                                        success = True
                                        break # 정상적으로 읽었으므로 재시도 루프 탈출
                                    else:
                                        success = True
                                        break # items 배열이 없지만 정상 응답이므로 탈출
                                elif header.get("resultCode") == "03": # No data
                                    success = True
                                    break # 데이터 없음 응답이므로 탈출
                                else:
                                    time.sleep(2)
                    except Exception as e:
                        print(f"KMA API {year} ({start_dt}-{end_dt}) error: {e}")
                        time.sleep(3)
                        
                if success and items_fetched < 999:
                    break # 현재 청크의 마지막 페이지이므로 페이지 루프 탈출
                    
            time.sleep(1) # delay between chunks
            
    if not records:
        return pd.DataFrame()
        
    df_raw = pd.DataFrame(records)
    
    # Filter to exact requested year bounds just in case chunks bleed over
    df_raw = df_raw[(df_raw['YEAR'] >= start_year) & (df_raw['YEAR'] <= end_year)]
    
    if df_raw.empty:
        return pd.DataFrame()
        
    pivot_df = df_raw.pivot_table(index=['STN_CD', 'YEAR', 'MONTH', 'DAY'], 
                                  columns='HOUR', 
                                  values='RF', 
                                  aggfunc='first').reset_index()
    
    for h in range(1, 25):
        if h not in pivot_df.columns:
            pivot_df[h] = 0.0
            
    hour_cols = {h: f"H{h}" for h in range(1, 25)}
    pivot_df = pivot_df.rename(columns=hour_cols)
    final_cols = ['STN_CD', 'YEAR', 'MONTH', 'DAY'] + [f"H{h}" for h in range(1, 25)]
    final_df = pivot_df[final_cols].fillna(0.0)
    
    return final_df

def fetch_kma_daily_max_rainfall(stn_cd, start_year, end_year, start_date=None, end_date=None):
    """
    Fetch daily 10-min and 60-min maximum rainfall data from KMA API.
    Returns a pandas DataFrame formatted: [STN_CD, YEAR, MONTH, DAY, MI10_MAX_RN, HR1_MAX_RN]
    """
    records = []
    
    # KMA STN_ID is usually the last 3 digits
    kma_cd = stn_cd[-3:].lstrip('0') if len(stn_cd) > 3 else stn_cd
    try:
        kma_cd = str(int(stn_cd[-3:]))
    except:
        pass
        
    if start_date and end_date:
        all_chunks = _generate_chunks(start_date, end_date)
    else:
        all_chunks = []
        for year in range(start_year, end_year + 1):
            all_chunks += [(f"{year}0101", f"{year}0630"), (f"{year}0701", f"{year}1231")]

    for start_dt, end_dt in all_chunks:
        url = f"http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList?serviceKey={KMA_KEY}&numOfRows=370&pageNo=1&dataType=JSON&dataCd=ASOS&dateCd=DAY&startDt={start_dt}&endDt={end_dt}&stnIds={kma_cd}"
        
        success = False
        for attempt in range(3):
            try:
                req = urllib.request.Request(url)
                with urllib.request.urlopen(req, timeout=20) as response:
                    if response.status == 200:
                        data = json.loads(response.read().decode('utf-8'))
                        header = data.get("response", {}).get("header", {})
                        
                        if header.get("resultCode") == "00":
                            items_dict = data.get("response", {}).get("body", {}).get("items", {})
                            if isinstance(items_dict, dict) and "item" in items_dict:
                                items = items_dict.get("item", [])
                                for item in items:
                                    tm = item.get("tm", "") # e.g. "2023-01-01"
                                    mi10 = item.get("mi10MaxRn", "")
                                    hr1 = item.get("hr1MaxRn", "")
                                    
                                    if len(tm) >= 10:
                                        y = int(tm[0:4])
                                        m = int(tm[5:7])
                                        d = int(tm[8:10])
                                        
                                        try:
                                            v_mi10 = float(mi10) if mi10 != "" else 0.0
                                        except ValueError:
                                            v_mi10 = 0.0
                                            
                                        try:
                                            v_hr1 = float(hr1) if hr1 != "" else 0.0
                                        except ValueError:
                                            v_hr1 = 0.0
                                            
                                        # Only store if either value > 0 to save space, or store all to be safe?
                                        # To be consistent with daily DB, store all days in range.
                                        records.append({
                                            "STN_CD": stn_cd,
                                            "YEAR": y,
                                            "MONTH": m,
                                            "DAY": d,
                                            "MI10_MAX_RN": v_mi10,
                                            "HR1_MAX_RN": v_hr1
                                        })
                                success = True
                                break
                            else:
                                success = True
                                break # array missing but valid response
                        elif header.get("resultCode") == "03":
                            success = True
                            break # No data
                        else:
                            time.sleep(2)
            except Exception as e:
                print(f"KMA Daily API ({start_dt}-{end_dt}) error: {e}")
                time.sleep(3)
                
        time.sleep(1)
        
    if not records:
        return pd.DataFrame(columns=["STN_CD", "YEAR", "MONTH", "DAY", "MI10_MAX_RN", "HR1_MAX_RN"])
        
    df = pd.DataFrame(records)
    return df

if __name__ == "__main__":
    print("Testing API Fetcher...")
    df = fetch_wamis_hourly_rainfall("10011100", 2018, 2018)
    if not df.empty:
        print(f"Successfully fetched {len(df)} days of data.")
        print(df.head())
    else:
        print("Failed to fetch data or empty result.")
