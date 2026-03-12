import pandas as pd
import numpy as np

def calculate_fixed_max_from_hourly(df_hourly):
    """
    시강우(Hourly) 데이터프레임을 받아 1~72시간 지속기간별 연최대 고정시간 강우량 산출
    입력: 'STN_CD', 'YEAR', 'MONTH', 'DAY', 'H1', 'H2', ... 'H24' 
    출력: 'STN_CD', 'YEAR', '1-HR', '2-HR', ... '72-HR'
    """
    if df_hourly.empty:
        return pd.DataFrame()
        
    df = df_hourly.copy()
    
    # 안전을 위해 컬럼 정리 (문자열 등 혼입 방지)
    for col in [f'H{i}' for i in range(1, 25)]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
    # 연도별, 관측소별 처리
    results = []
    
    for (stn, yr), group in df.groupby(['STN_CD', 'YEAR']):
        # 그룹 데이터를 시간순으로 정렬 (월, 일 기준)
        group = group.sort_values(by=['MONTH', 'DAY']).reset_index(drop=True)
        
        # 1년치 시간당 강우량을 1차원 배열로 평탄화 (Flatten)
        # H1 ~ H24 값을 순서대로 나열
        hourly_cols = [f'H{i}' for i in range(1, 25)]
        
        # 데이터를 1차원으로 만듦: [관측소1_1월1일_H1, ..., 관측소1_12월31일_H24]
        flat_data = group[hourly_cols].to_numpy().flatten()
        
        # pandas Series 로 변환하여 rolling 연산 수행
        ts = pd.Series(flat_data)
        
        year_max_record = {
            'STN_CD': stn,
            'YEAR': yr
        }
        
        for duration in range(1, 73):
            if len(ts) >= duration:
                # 윈도우 사이즈(duration)만큼 이동 합계를 구하고 그 중 최댓값
                max_val = ts.rolling(window=duration, min_periods=1).sum().max()
            else:
                max_val = 0
                
            # 소수점 1자리 반올림 (포맷 유지)
            year_max_record[f'{duration}-HR'] = np.floor(max_val * 10 + 0.5) / 10 if pd.notna(max_val) else 0.0
            
        results.append(year_max_record)
        
    return pd.DataFrame(results)

def convert_to_arbitrary_max(fixed_df):
    """
    고정시간 최대강우량(fixed_df)을 받아 임의시간 최대강우량으로 환산.
    환산계수 적용 및 역전보간 알고리즘 (build_past_db.py 의 로직 재사용)
    """
    if fixed_df.empty:
        return pd.DataFrame()
        
    factors = np.ones(72)
    for i in range(1, 49): # 1~48시간
        if i == 1: factors[i-1] = 1.136
        elif i == 2: factors[i-1] = 1.051
        elif i == 3: factors[i-1] = 1.031
        elif i == 4: factors[i-1] = 1.020
        elif i == 6: factors[i-1] = 1.012
        elif i == 9: factors[i-1] = 1.007
        elif i == 12: factors[i-1] = 1.005
        elif i == 18: factors[i-1] = 1.004
        elif i == 24: factors[i-1] = 1.003
        elif i == 48: factors[i-1] = 1.002
        else:
            factors[i-1] = 0.1346 * (i ** -1.417) + 1.0014
            
    arb_df = fixed_df.copy()
    columns_to_convert = [f"{i}-HR" for i in range(1, 73)]
    
    for i, col in enumerate(columns_to_convert):
        if col in arb_df.columns:
            if i < 48:
                val = arb_df[col] * factors[i]
                arb_df[col] = np.floor(val * 10 + 0.5) / 10
                
    # 역전보간 (다음 시간 값이 현재 시간 값보다 크면 현재 값으로 덮어씀. 누적 개념이므로 지속시간이 길수록 값이 크거나 같아야 함 -> 여기서는 반대로 1시간이 작은걸 방지?)
    # 기존 코드 분석: mask = arb_df[next_col] < arb_df[cur_col] 일때 next에 cur를 덮어씀 (즉, 큰 시간 윈도우의 강우량이 작은 시간 윈도우보다 작아지는 모순 방지)
    for i in range(1, 72):
        cur_col = f"{i}-HR"
        next_col = f"{i+1}-HR"
        if cur_col in arb_df.columns and next_col in arb_df.columns:
            mask = arb_df[next_col] < arb_df[cur_col]
            arb_df.loc[mask, next_col] = arb_df.loc[mask, cur_col]
            
    return arb_df

def convert_to_arbitrary_max_with_kma_yearly(fixed_df, df_kma_yearly):
    """
    고정시간 최대강우량(fixed_df)에 임의시간 환산계수만 전부 곱한 뒤,
    1시간(1-HR) 칼럼의 값만 기상청 연최대 60분 강우량(HR1_MAX_RN) 데이터로 대체(덮어쓰기)합니다.
    그 이후에 역전보간 보정을 거쳐 최종 수정된 임의시간 데이터프레임을 반환합니다.
    (기상청 1시간 자료에는 환산계수 1.136이 곱해지지 않습니다)
    """
    if fixed_df.empty:
        return pd.DataFrame()
        
    factors = np.ones(72)
    for i in range(1, 49): # 1~48시간
        if i == 1: factors[i-1] = 1.136
        elif i == 2: factors[i-1] = 1.051
        elif i == 3: factors[i-1] = 1.031
        elif i == 4: factors[i-1] = 1.020
        elif i == 6: factors[i-1] = 1.012
        elif i == 9: factors[i-1] = 1.007
        elif i == 12: factors[i-1] = 1.005
        elif i == 18: factors[i-1] = 1.004
        elif i == 24: factors[i-1] = 1.003
        elif i == 48: factors[i-1] = 1.002
        else:
            factors[i-1] = 0.1346 * (i ** -1.417) + 1.0014
            
    arb_df = fixed_df.copy()
    columns_to_convert = [f"{i}-HR" for i in range(1, 73)]
    
    # 1. 환산계수 일괄 적용
    for i, col in enumerate(columns_to_convert):
        if col in arb_df.columns:
            if i < 48:
                val = arb_df[col] * factors[i]
                arb_df[col] = np.floor(val * 10 + 0.5) / 10
                
    # 2. 기상청 60분 최대치로 1-HR 값 덮어쓰기 (계수 미적용 순수 원본값 삽입)
    if not df_kma_yearly.empty and '1-HR' in arb_df.columns:
        # 조인을 위해 두 데이터프레임을 병합
        merged = pd.merge(arb_df, df_kma_yearly[['STN_CD', 'YEAR', 'HR1_MAX_RN']], on=['STN_CD', 'YEAR'], how='left')
        
        # 기상청 값이 유효한지(Not NA, >0) 검증할 마스크 생성
        valid_mask = merged['HR1_MAX_RN'].notna() & (merged['HR1_MAX_RN'] > 0)
        
        # 유효한 기상청 값으로 1-HR 값을 대체
        arb_df.loc[valid_mask, '1-HR'] = merged.loc[valid_mask, 'HR1_MAX_RN']
        
    # 3. 역전보간 적용
    for i in range(1, 72):
        cur_col = f"{i}-HR"
        next_col = f"{i+1}-HR"
        if cur_col in arb_df.columns and next_col in arb_df.columns:
            mask = arb_df[next_col] < arb_df[cur_col]
            arb_df.loc[mask, next_col] = arb_df.loc[mask, cur_col]
            
    return arb_df

def process_hourly_to_max(df_hourly):
    """
    1. 시강우 -> 고정시간
    2. 고정시간 -> 임의시간
    순차적으로 파이프라인 처리하여 두 데이터프레임을 반환
    """
    df_fixed = calculate_fixed_max_from_hourly(df_hourly)
    df_arb = convert_to_arbitrary_max(df_fixed)
    return df_fixed, df_arb
