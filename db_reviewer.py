import pandas as pd
import os

DB_DIR = "./data/db_versions"

def generate_db_review_report(version_name, save_path):
    """
    특정 DB 버전 폴더의 3가지 Parquet 파일을 읽어 교차 검증 및 결측치 요약 엑셀 리포트를 생성합니다.
    """
    target_dir = os.path.join(DB_DIR, version_name)
    
    hourly_path = os.path.join(target_dir, "hourly.parquet")
    fixed_path = os.path.join(target_dir, "fixed_max.parquet")
    arb_path = os.path.join(target_dir, "arb_max.parquet")
    
    # 데이터 로드
    df_h, df_f, df_a, df_k, df_y, df_m = pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    
    if os.path.exists(hourly_path): df_h = pd.read_parquet(hourly_path)
    if os.path.exists(fixed_path): df_f = pd.read_parquet(fixed_path)
    if os.path.exists(arb_path): df_a = pd.read_parquet(arb_path)
    
    kma_daily_path = os.path.join(target_dir, "kma_daily_max.parquet")
    if os.path.exists(kma_daily_path): df_k = pd.read_parquet(kma_daily_path)

    kma_yearly_path = os.path.join(target_dir, "kma_yearly_max.parquet")
    if os.path.exists(kma_yearly_path): df_y = pd.read_parquet(kma_yearly_path)
    
    arb_mod_path = os.path.join(target_dir, "arb_max_modified.parquet")
    if os.path.exists(arb_mod_path): df_m = pd.read_parquet(arb_mod_path)
    
    # 관측소 마스터 정보 로드 (관할기관 정보 획득용)
    master_path = "./data/강우관측소(지역빈도).xlsx"
    org_dict = {}
    if os.path.exists(master_path):
        try:
            m_df = pd.read_excel(master_path)
            # 0번 컬럼: 관측소코드, 4번 컬럼: 관할기관
            code_col = m_df.columns[0]
            org_col = m_df.columns[4]
            for _, row in m_df.iterrows():
                c_val = str(row[code_col]).strip()
                if c_val.endswith('.0'): c_val = c_val[:-2]
                o_val = str(row[org_col]).strip() if pd.notna(row[org_col]) else ""
                org_dict[c_val] = o_val
        except Exception as e:
            print(f"마스터 엑셀 로드 에러: {e}")
    
    # 1. 시강우 결측 분석
    # YEAR 단위로 그룹화하여 해당 연도의 총 레코드 수 확인 (보통 윤년 제외 365, 윤년 366). 
    # 혹은 단순 존재 유무로 '보유 연도', '빠진 연도' 판단.
    
    summary_data = []
    
    # 전체 등장 관측소 통합
    all_stations = set()
    for df in [df_h, df_f, df_a, df_k, df_y, df_m]:
        if not df.empty and 'STN_CD' in df.columns:
            all_stations.update(df['STN_CD'].astype(str).unique())
            
    all_stations = sorted(list(all_stations))
    
    for stn in all_stations:
        org_name = org_dict.get(stn, "알수없음")
        stn_info = {
            '관측소코드': stn,
            '관할기관': org_name
        }
        
        # 시강우(Hourly) 연도 분석
        if not df_h.empty and 'STN_CD' in df_h.columns:
            h_stn = df_h[df_h['STN_CD'].astype(str) == stn]
            if not h_stn.empty:
                years = sorted(h_stn['YEAR'].dropna().unique())
                stn_info['[시강우] 시작~종료'] = f"{int(years[0])}~{int(years[-1])}" if years else "없음"
                missing_years = [y for y in range(int(years[0]), int(years[-1]) + 1) if y not in years] if len(years) > 1 else []
                stn_info['[시강우] 누락 연도'] = ", ".join(map(str, missing_years)) if missing_years else "없음"
                stn_info['[시강우] 총 레코드수(일)'] = len(h_stn)
            else:
                stn_info['[시강우] 시작~종료'] = "데이터 없음"
                stn_info['[시강우] 누락 연도'] = "-"
                stn_info['[시강우] 총 레코드수(일)'] = 0
        else:
            stn_info['[시강우] 시작~종료'] = "파일 없음"
            
        # 고정시간(Fixed Max) 연도 분석 
        if not df_f.empty and 'STN_CD' in df_f.columns:
            f_stn = df_f[df_f['STN_CD'].astype(str) == stn]
            if not f_stn.empty and 'YEAR' in f_stn.columns:
                years = sorted(f_stn['YEAR'].dropna().unique())
                stn_info['[고정시간] 보유 연도 수'] = len(years)
                stn_info['[고정시간] 시작~종료'] = f"{int(years[0])}~{int(years[-1])}" if years else "없음"
            else:
                stn_info['[고정시간] 시작~종료'] = "데이터 없음"
                
        # 임의시간(Arb Max) 연도 분석
        if not df_a.empty and 'STN_CD' in df_a.columns:
            a_stn = df_a[df_a['STN_CD'].astype(str) == stn]
            if not a_stn.empty and 'YEAR' in a_stn.columns:
                years = sorted(a_stn['YEAR'].dropna().unique())
                stn_info['[임의시간] 보유 연도 수'] = len(years)
            else:
                stn_info['[임의시간] 보유 연도 수'] = 0
                
        # 6번 파케이: 수정임의시간(Arb Max Modified) 연도 분석
        m_rcd = "데이터 없음"
        if not df_m.empty and 'STN_CD' in df_m.columns:
            m_stn = df_m[df_m['STN_CD'].astype(str) == stn]
            if not m_stn.empty and 'YEAR' in m_stn.columns:
                years = sorted(m_stn['YEAR'].dropna().unique())
                stn_info['[수정임의] 보유 연도 수'] = len(years)
                m_rcd = f"{int(years[0])}~{int(years[-1])}" if years else "없음"
                stn_info['[수정임의] 시작~종료'] = m_rcd
            else:
                stn_info['[수정임의] 보유 연도 수'] = 0
                stn_info['[수정임의] 시작~종료'] = m_rcd
        else:
            stn_info['[수정임의] 보유 연도 수'] = 0
            stn_info['[수정임의] 시작~종료'] = "파일 없음"
                
        # 기상청 일자료(10/60분 최대) 연도 분석
        k_rcd = "데이터 없음"
        if not df_k.empty and 'STN_CD' in df_k.columns:
            k_stn = df_k[df_k['STN_CD'].astype(str) == stn]
            if not k_stn.empty and 'YEAR' in k_stn.columns:
                years = sorted(k_stn['YEAR'].dropna().unique())
                stn_info['[기상청일자료] 보유 연도 수'] = len(years)
                k_rcd = f"{int(years[0])}~{int(years[-1])}" if years else "없음"
                stn_info['[기상청일자료] 시작~종료'] = k_rcd
                
                missing_years = [y for y in range(int(years[0]), int(years[-1]) + 1) if y not in years] if len(years) > 1 else []
                stn_info['[기상청일자료] 누락 연도'] = ", ".join(map(str, missing_years)) if missing_years else "없음"
            else:
                stn_info['[기상청일자료] 보유 연도 수'] = 0
                stn_info['[기상청일자료] 시작~종료'] = k_rcd
                stn_info['[기상청일자료] 누락 연도'] = "-"
        else:
            stn_info['[기상청일자료] 보유 연도 수'] = 0
            stn_info['[기상청일자료] 시작~종료'] = "파일 없음"
            stn_info['[기상청일자료] 누락 연도'] = "-"
        # 기상청 연자료(10/60분 연최대치) 분석
        y_rcd = "데이터 없음"
        if not df_y.empty and 'STN_CD' in df_y.columns:
            y_stn = df_y[df_y['STN_CD'].astype(str) == stn]
            if not y_stn.empty and 'YEAR' in y_stn.columns:
                years = sorted(y_stn['YEAR'].dropna().unique())
                stn_info['[기상청연자료] 보유 연도 수'] = len(years)
                y_rcd = f"{int(years[0])}~{int(years[-1])}" if years else "없음"
                stn_info['[기상청연자료] 시작~종료'] = y_rcd
                
                missing_years = [y for y in range(int(years[0]), int(years[-1]) + 1) if y not in years] if len(years) > 1 else []
                stn_info['[기상청연자료] 누락 연도'] = ", ".join(map(str, missing_years)) if missing_years else "없음"
            else:
                stn_info['[기상청연자료] 보유 연도 수'] = 0
                stn_info['[기상청연자료] 시작~종료'] = y_rcd
                stn_info['[기상청연자료] 누락 연도'] = "-"
        else:
            stn_info['[기상청연자료] 보유 연도 수'] = 0
            stn_info['[기상청연자료] 시작~종료'] = "파일 없음"
            stn_info['[기상청연자료] 누락 연도'] = "-"
                
        # 교차 검증(Cross Validation) - 시강우 연도 수와 최대강우 연도 수가 일치하는가?
        h_rcd = stn_info.get('[시강우] 시작~종료', '')
        f_rcd = stn_info.get('[고정시간] 시작~종료', '')
        
        # 1차 검증: 시강우 vs 고정/임의 최대강우
        if h_rcd == "데이터 없음" and f_rcd != "데이터 없음":
            base_status = "시강우 누락 (최대강우만 존재)"
        elif h_rcd != "데이터 없음" and f_rcd == "데이터 없음":
            base_status = "최대강우 누락 (시강우만 존재)"
        elif h_rcd != f_rcd:
            base_status = f"기간 불일치 (시강우:{h_rcd} vs 고정:{f_rcd})"
        else:
            base_status = "정상"
            
        # 2차 검증: 기상청 일자료 및 연자료 검증 (관할기관 KMA 여부 체크)
        if "기상청" in org_name:
            if k_rcd == "데이터 없음" or k_rcd == "파일 없음":
                stn_info['교차검증 이상여부'] = f"{base_status} / 기상청 10·60분 누락(전여부)"
            elif k_rcd != y_rcd:
                stn_info['교차검증 이상여부'] = f"{base_status} / 기상청 일자료-연자료 불일치 / 수정임의:{m_rcd}"
            elif h_rcd != k_rcd and h_rcd != "데이터 없음":
                stn_info['교차검증 이상여부'] = f"{base_status} / 기상청 기간 불일치(시강우:{h_rcd} vs 기상청:{k_rcd}) / 수정임의:{m_rcd}"
            else:
                stn_info['교차검증 이상여부'] = f"{base_status} (기상청 1~6번 파케이 완벽 동기화됨 - 수정임의:{m_rcd})"
        else:
            # 기상청 관측소가 아닌 경우 4~6번 추가 파케이가 없는 것이 정상일 수도 있고, WAMIS 6번 파케이는 원본과 동일하게 찰 것
            if base_status == "정상":
                stn_info['교차검증 이상여부'] = "정상 (WAMIS 관할로 기상청 특수DB 제외)"
            else:
                stn_info['교차검증 이상여부'] = f"{base_status} (WAMIS 관할)"
            
        summary_data.append(stn_info)
        
    df_report = pd.DataFrame(summary_data)
    
    # 엑셀 파일 작성
    with pd.ExcelWriter(save_path, engine='xlsxwriter') as writer:
        # 요약 리포트 시트
        df_report.to_excel(writer, sheet_name='관측소별 데이터 결측 조회', index=False)
        
        # 서식 지정 (열 너비 등)
        worksheet = writer.sheets['관측소별 데이터 결측 조회']
        worksheet.set_column('A:B', 15)   # 코드, 관할기관
        worksheet.set_column('C:E', 20)   # 시강우 기간, 누락, 레코드수
        worksheet.set_column('F:H', 20)   # 고정, 임의 기간
        worksheet.set_column('I:K', 20)   # 일자료 기간, 누락, 레코드수
        worksheet.set_column('L:N', 20)   # 연자료 기간, 누락, 레코드수
        worksheet.set_column('O:P', 20)   # 수정임의 보유, 기간
        worksheet.set_column('Q:Q', 60)   # 교차검증 상태

    return True, f"{len(all_stations)}개 관측소의 진단 리포트가 성공적으로 저장되었습니다."

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        generate_db_review_report(sys.argv[1], "test_report.xlsx")
        print("Done.")
