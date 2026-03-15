# 강우자료 통합 관리 및 분석 시스템 v2.0

**개발자:** 김찬영 | **버전:** 2.0 | **초기 작성:** 2023.11.28 | **최종 개정:** 2026.03.16

---

## 1. 프로젝트 개요

한국의 강우 관측 데이터를 WAMIS(수자원공사) 및 KMA(기상청) 공공 API에서 자동 수집하고,
Parquet 형식의 로컬 DB로 관리/분석하는 PyQt6 기반 데스크톱 통합 플랫폼.

| 기능 | 설명 |
|------|------|
| API 데이터 수집 | WAMIS / KMA ASOS API에서 시강우/최대강우 수집 |
| 자동 강우량 산출 | 시강우 -> 고정시간 -> 임의시간 최대강우량 자동 계산 |
| Parquet DB 관리 | 연도별 버전으로 6종 Parquet 파일 세트 관리 |
| DB 버전 병합 | 두 DB 버전을 하나로 병합 |
| 엑셀 추출 | 관측소/연도 선택 후 6종 엑셀 파일로 군집별 추출 |
| DB 편집 | GUI로 Parquet 파일 직접 편집 |
| 품질 검토 리포트 | DB 결측 현황 엑셀 리포트 자동 생성 |

- **관측소:** 약 400~500개, 1~26번 군집(Cluster)으로 분류
- **관할기관 구분:** 기상청(KMA ASOS) / 나머지(WAMIS)

---

## 2. 파일 및 폴더 구조

```
Rainfall/
├── main_app.py                     # 메인 PyQt6 GUI (1,397줄)
├── api_fetcher.py                  # WAMIS/KMA API 수집 (315줄)
├── max_rainfall_calculator.py      # 강우량 계산/환산 (163줄)
├── db_reviewer.py                  # DB 검토 리포트 (218줄)
├── fetch_station_db.py             # WAMIS API 기반 관측소 제원 수집 (245줄)
├── convert_sta_to_parquet.py       # 엑셀 관측소 DB를 Parquet으로 변환 (226줄)
├── Sta1_db.parquet                 # 내장 DB 기반 엑셀 추출 시 사용하는 관측소 DB
├── Sta2_db.parquet                 # 외부 API 실시간 다운로드 시 사용하는 관측소 DB
└── data/
    ├── final_DB_Review_Report.xlsx # 최종 DB 검토 리포트
    └── db_versions/
        └── {버전명}/               # DB 버전 폴더
            ├── hourly.parquet      # 시강우 원본
            ├── fixed_max.parquet   # 고정시간 최대강우
            ├── arb_max.parquet     # 임의시간 최대강우
            ├── arb_max_modified.parquet  # 수정 임의시간 (KMA 1시간 적용)
            ├── kma_daily_max.parquet     # KMA 일별 10분/60분 최대
            └── kma_yearly_max.parquet    # KMA 연최대 10분/60분
```

---

## 3. 기술 스택

| 라이브러리 | 용도 |
|-----------|------|
| PyQt6 | GUI 프레임워크 |
| qt-material | Material Design 테마 |
| pandas | 데이터 조작, Excel/Parquet I/O |
| numpy | Rolling window, 배열 연산 |
| urllib / json | HTTP API 요청 및 JSON 파싱 |
| xlsxwriter | 서식 있는 Excel 생성 |
| concurrent.futures | 멀티스레딩 (API 병렬 요청) |
| QThread | 백그라운드 다운로드 스레드 |

**Python 버전:** Python 3.8+

---

## 4. 주요 모듈 상세

### 4.1 main_app.py -- 메인 GUI

다섯 개의 클래스로 구성된다.

**PandasModel (25~66줄)**
- QAbstractTableModel 상속
- DataFrame을 QTableView에 표시
- setData()로 셀 직접 편집 지원 (DB 편집 탭)

**ApiDownloadThread (67~220줄)**
- QThread 상속, API 수집을 백그라운드에서 처리
- 시그널: log_signal(로그), progress_signal(진행률%), finished_signal(완료)
- 수집 모드 1: 시강우 DB 세트 전체 (hourly -> fixed_max -> arb_max -> kma_daily/yearly_max -> arb_max_modified)
- 수집 모드 2: KMA 일/연최대만 신규 생성

**FetchStationThread (221~269줄)**
- QThread 상속, 관측소 제원 수집을 백그라운드에서 처리

**ConvertStaThread (270~300줄)**
- QThread 상속, 엑셀 관측소 DB → Parquet 변환을 백그라운드에서 처리

**RainfallApp (301~1397줄)**
- QMainWindow 상속, 1200x800px
- 좌측 패널: 관측소 선택 (검색창, 군집 체크박스 1~26, 관측소 테이블)
- 우측 패널: 기능 탭 + 실행 로그 콘솔 + 진행 상태바

| 탭 | 접근 | 설명 |
|----|------|------|
| 내장 DB 기반 엑셀 추출 | 공개 | DB에서 6종 Excel 추출 |
| 외부 API 실시간 다운로드 | 공개 | API 수집 후 즉시 Excel 저장 |
| DB 컴포넌트 관리 및 병합 | 관리자 | DB 버전 병합, 리포트 생성 |
| DB 편집 | 관리자 | Parquet 직접 편집 |
| 내장 DB 연도별 업데이트 | 관리자 | 신규 DB 버전 생성 |

> 관리자 모드: 타이틀 라벨 5회 클릭 (eventFilter 구현)

**주요 메서드:**

| 메서드 | 기능 |
|--------|------|
| _load_station_table(path) | parquet_path에 따라 관측소 테이블 재구성 (탭 전환 시 호출, 체크박스·검색창 초기화 포함) |
| filter_by_search() | 검색어 필터링 |
| on_cluster_checked() | 군집 선택 시 하위 관측소 자동 체크 |
| run_admin_db_update() | 신규 DB 버전 생성 |
| run_db_merge() | DB 버전 병합 |
| run_db_report() | DB 검토 리포트 생성 |
| run_db_extraction() | 내장 DB 선택 및 검증 후 _do_extraction 호출 |
| _do_extraction(db_path, ...) | Parquet DB 로드 → 6종 Excel 생성 (내장 DB / API 임시 DB 공통 재사용) |
| run_api_download() | API 수집 → 임시 DB → _do_extraction → 임시 DB 삭제 |
| _cleanup_temp_db() | data/temp/api_download_temp/ 자동 삭제 |

---

### 4.2 api_fetcher.py -- API 수집

**_generate_chunks(start_date, end_date)**
- YYYYMMDD 형식 날짜 범위를 6개월 단위 청크 리스트로 변환
- start_date/end_date 지정 시 사용; 미지정 시 연도 루프 방식으로 대체

**fetch_wamis_hourly_rainfall(stn_cd, start_year, end_year, start_date=None, end_date=None)**
- 엔드포인트: http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_hrdata
- start_date/end_date 지정 시 _generate_chunks 기반 날짜 범위 청크 호출, 미지정 시 연도별 상하반기 분할
- 응답 파싱: ymdh 10자리 문자열 -> YEAR/MONTH/DAY/HOUR
- 반환: [STN_CD, YEAR, MONTH, DAY, H1~H24] DataFrame

**fetch_kma_hourly_rainfall(stn_cd, start_year, end_year, start_date=None, end_date=None)**
- 엔드포인트: http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList
- start_date/end_date 선택 파라미터 동일 적용
- 자정(00:00) 강우 -> 전날 H24로 변환 (KMA 시간 규칙)
- 응답 코드: 00=정상, 03=데이터없음

**fetch_kma_daily_max_rainfall(stn_cd, start_year, end_year, start_date=None, end_date=None)**
- 엔드포인트: http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList
- start_date/end_date 선택 파라미터 동일 적용
- 반환: [STN_CD, YEAR, MONTH, DAY, MI10_MAX_RN, HR1_MAX_RN]

공통 처리: 최대 3회 재시도, 타임아웃 15~20초, 청크 간 1~3초 딜레이

---

### 4.3 max_rainfall_calculator.py -- 강우량 계산

**calculate_fixed_max_from_hourly(df_hourly)**
- 1~72시간 지속기간별 연최대강우량 산출
- 패턴: rolling(window=N).sum().max()
- 반환: [STN_CD, YEAR, 1-HR~72-HR]

**convert_to_arbitrary_max(fixed_df)**
- 고정시간 강우량 -> 임의시간 강우량 환산계수 적용

| 지속기간 | 환산계수 |
|---------|----------|
| 1-HR | 1.136 |
| 2-HR | 1.051 |
| 3-HR | 1.031 |
| 4-HR | 1.020 |
| 6-HR | 1.012 |
| 9-HR | 1.007 |
| 12-HR | 1.005 |
| 18-HR | 1.004 |
| 24-HR | 1.003 |
| 48-HR | 1.002 |
| 기타 | 0.1346 x i^(-1.417) + 1.0014 |

역전보간: 긴 지속기간 < 짧은 지속기간인 모순 발생 시 자동 보정

**convert_to_arbitrary_max_with_kma_yearly(fixed_df, df_kma_yearly)**
- 임의시간 환산 후 1-HR만 KMA 60분 최대(HR1_MAX_RN)로 대체
- 대체 후 역전보간 재적용

**process_hourly_to_max(df_hourly)**
- 파이프라인: 시강우 -> 고정시간 -> 임의시간

---

### 4.4 db_reviewer.py -- DB 검토

**generate_db_review_report(version_name, save_path)**
- 6개 Parquet 파일 교차 검증 -> 엑셀 리포트 생성

| 검증 항목 | 내용 |
|---------|------|
| 시강우 | 시작~종료 연도, 누락 연도, 레코드 수 |
| 고정시간 최대강우 | 보유 연도 범위, 누락 연도 |
| 임의시간 최대강우 | 보유 연도 범위 |
| KMA 일자료 | 10분/60분 최대 보유 연도, 누락 연도 |
| KMA 연자료 | 연최대 보유 연도 |
| 수정 임의시간 | 보유 연도 범위 |

교차 검증: 시강우 기간 <-> 최대강우량 기간 일치, 기상청 관측소 KMA DB 완전성 확인

---

## 5. 데이터 구조

### Parquet DB 파일 스키마

| 파일 | 주요 컬럼 | 설명 |
|------|----------|------|
| hourly.parquet | STN_CD, YEAR, MONTH, DAY, H1~H24 | 시간별 강우량 (mm) |
| fixed_max.parquet | STN_CD, YEAR, 1-HR~72-HR | 고정시간 연최대강우량 |
| arb_max.parquet | STN_CD, YEAR, 1-HR~72-HR | 임의시간 연최대강우량 |
| kma_daily_max.parquet | STN_CD, YEAR, MONTH, DAY, MI10_MAX_RN, HR1_MAX_RN | KMA 일별 10분/60분 최대 |
| kma_yearly_max.parquet | STN_CD, YEAR, MI10_MAX_RN, HR1_MAX_RN | KMA 연최대 |
| arb_max_modified.parquet | STN_CD, YEAR, 1-HR~72-HR | 수정 임의시간 (KMA 1-HR 대체) |

---

## 6. 데이터 흐름

```
[WAMIS API]    [KMA ASOS API]
     |                |
     +-------+--------+
             | 시강우 (H1~H24)
             v
     hourly.parquet (STN, YEAR/MONTH/DAY, H1~H24)
             |
             | Rolling Window 1~72시간
             v
     fixed_max.parquet (STN, YEAR, 1-HR~72-HR)
             |
             | 환산계수 + 역전보간
             v
     arb_max.parquet (STN, YEAR, 1-HR~72-HR)
             |
     +-------+------> KMA 60분 1-HR 대체 --> arb_max_modified.parquet
     |
     +-> kma_daily_max.parquet  (일별 MI10/HR1)
     +-> kma_yearly_max.parquet (연최대 MI10/HR1)

             | 관측소/연도 선택
             v
     엑셀 추출 결과 (6종, 군집별 폴더)
       1. 시강우.xlsx
       2. 고정시간최대강우.xlsx
       3. 임의시간최대강우.xlsx
       1.1 기상청10_60min 강우.xlsx
       2.1 기상청10_60min 연최대.xlsx
       3.1 임의시간최대강우_기상청.xlsx
```

---

## 7. API 통신 상세

**WAMIS API:**
- 엔드포인트: http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_hrdata
- 파라미터: obscd, startdt(YYYYMMDD), enddt(YYYYMMDD), output=json, key
- 응답: {"result":{"code":"success"}, "list":[{"ymdh":"2023010101","rf":"0.5"},...]}
- 파싱: ymdh[0:4]=YEAR, [4:6]=MONTH, [6:8]=DAY, [8:10]=HOUR

**KMA ASOS API - 시강우:**
- 엔드포인트: http://apis.data.go.kr/1360000/AsosHourlyInfoService/getWthrDataList
- 파라미터: serviceKey, numOfRows=999, pageNo, dataType=JSON, dataCd=ASOS, dateCd=HR, stnIds, startDt, endDt
- 응답 코드: 00=정상, 03=데이터없음 / 특이사항: 자정(00:00) -> 전날 H24로 변환

**KMA ASOS API - 일별 최대강우:**
- 엔드포인트: http://apis.data.go.kr/1360000/AsosDalyInfoService/getWthrDataList
- 파라미터: serviceKey, numOfRows=370, pageNo=1, dataType=JSON, dataCd=ASOS, dateCd=DAY, stnIds, startDt, endDt
- 응답 필드: tm(날짜), mi10MaxRn(10분 최대), hr1MaxRn(1시간 최대)

---

## 8. UI 구조 및 기능

**윈도우:** 1200x800px | **테마:** qt-material 녹색 (primaryColor: #1c9432)

```
+---------------------------------------------------------------+
| 타이틀 라벨 (5번 클릭 -> 관리자 모드 활성화)                   |
+--------------------+------------------------------------------+
| 좌측: 관측소 패널   | 우측: 기능 탭                             |
|                    |                                          |
| [검색창]           | [탭1] 내장 DB 기반 엑셀 추출              |
| [군집 체크박스]     | [탭2] 외부 API 실시간 다운로드            |
|  1~26군집          | [탭3] DB 컴포넌트 관리 (관리자)           |
| [관측소 테이블]     | [탭4] DB 편집 (관리자)                   |
|  코드/이름/유역/기관| [탭5] DB 연도별 업데이트 (관리자)         |
|                    |                                          |
|                    | [실행 로그 콘솔]                          |
|                    | [진행 상태바]                             |
+--------------------+------------------------------------------+
```

---

## 9. DB 버전 관리

- **버전 명명:** ~{연도}/ (예: ~2025/)
- **병합:** pd.concat + drop_duplicates(subset=[STN_CD, YEAR, ...])

**신규 DB 생성 순서:**
1. 버전명 입력
2. 관측소 선택
3. ApiDownloadThread 백그라운드 실행
4. data/db_versions/{버전명}/ 자동 생성
5. 6개 Parquet 파일 저장

---

## 10. 설정 및 상수

**main_app.py 경로 상수:**
```python
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR   = os.path.join(BASE_DIR, "data", "db_versions")
TEMP_DIR = os.path.join(BASE_DIR, "data", "temp")   # API 임시 DB 저장 경로
APP_VERSION = "2.0"
APP_AUTHOR  = "김찬영"
APP_DATE    = "2023.11.28. (개정 2026.03)"
```

**custom_theme.xml 색상:**
- primaryColor: #1c9432 (진한 초록)
- primaryLightColor: #58c65f (밝은 초록)
- secondaryColor: #FFFDF5 (크림 배경)

**API 키:** api_fetcher.py 상단에 하드코딩

---

---

## 11. 알려진 버그 및 보류 사항

| 항목 | 위치 | 상태 |
|------|------|------|
| KMA 관측소 종료일=오늘 설정 시 시강우·일일최다강우 빈 반환 | `api_fetcher.py` L:144, L:260 | 보류 |

**KMA 버그 상세:**
KMA API는 전일(T-1)까지만 데이터를 제공한다. 종료일이 오늘이면 API가 에러 코드를 반환하고, 재시도 3회 소진 후 해당 청크 전체가 누락된다. 날짜 범위가 당해 연도만 포함될 경우 단일 청크 전체 실패로 빈 DataFrame이 반환된다.
수정 방향: KMA 두 함수에서 `end_date`를 `min(end_date, 어제)`로 자동 보정.

---

## 12. 배포 계획 (docs/shareplan.md 참조)

| Phase | 내용 | 상태 |
|-------|------|------|
| 1 | `requirements.txt` 생성 | 예정 |
| 2 | PyInstaller `rainfall.spec` → `RainFall.exe` 빌드 | 예정 |
| 3 | Inno Setup `installer.iss` → `RainFall_Setup_v2.0.exe` | 예정 |
| 4 | `updater.py` + GitHub Releases 기반 앱/DB 자동 업데이트 | 예정 |

*문서 최종 수정일: 2026-03-16*
