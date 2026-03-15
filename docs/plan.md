# plan

## 완료 이력

### API 실시간 다운로드 ✅ (2026-03-12)
- [x] 관측소 DB PARQUET 탭 연동
    - `Sta1_db.parquet`: 내장 DB 탭 활성 시 left panel에 로딩
    - `Sta2_db.parquet`: 외부 API 탭 활성 시 left panel에 로딩
    - 탭 전환 시 관측소 목록 교체 + 체크박스/검색창 전체 초기화
- [x] `main_app.py` UI 수정
    - 추출 기간 INPUT 수정: 시작연도~종료연도 → 시작 연월일~종료 연월일 (QDateEdit)
    - `run_db_extraction` → `_do_extraction(db_path, ...)` 분리 (내장DB/API 공통 재사용)
    - `_load_station_table(parquet_path)` 범용화 (기존 `load_station_data` 대체)
- [x] `api_fetcher.py` 날짜 범위 기반 청크 분할 호출
    - `_generate_chunks(start_date, end_date)` 헬퍼 추가
    - 세 함수에 `start_date`, `end_date` 선택 파라미터 추가 (하위 호환 유지)
    - 내장 DB 생성 경로(`run_admin_db_update`)는 기존 연도 방식 그대로 유지
- [x] 임시 Parquet DB 생성 → 엑셀 추출 → 임시 DB 자동 삭제 파이프라인
    - 임시 저장 경로: `data/temp/api_download_temp/`
    - `_cleanup_temp_db()`로 성공/실패 모두 자동 삭제

---

## 알려진 버그 (보류)

### KMA 관측소 API 종료일=오늘 설정 시 빈 결과 반환 (2026-03-16 파악)
- **증상:** 기상청 관측소 선택 후 종료일을 당일로 설정하면 시강우·일일최다강우 모두 빈 DataFrame 반환
- **원인:**
  - KMA API는 전일(T-1)까지만 데이터 제공
  - `fetch_kma_hourly_rainfall`: `kma_end_dt = end_dt + 1일`로 요청 → 미래 날짜 요청 시 에러 코드 반환 → 재시도 3회 소진 후 청크 전체 누락
  - `fetch_kma_daily_max_rainfall`: `endDt=오늘`로 요청 → 동일 문제
  - 날짜 범위가 당해 연도만 포함될 경우 단일 청크 전체 실패 → 빈 반환
- **위치:** `api_fetcher.py` L:144 (hourly), L:260 (daily)
- **수정 방향:** KMA 함수 내 `end_date`를 `min(end_date, yesterday)`로 자동 보정

기상청 api로 수정?

---

## 현재 목표: 배포 준비 (docs/shareplan.md 상세)

### Phase 1: 빌드 환경 세팅
- [ ] `requirements.txt` 생성

### Phase 2: PyInstaller EXE 빌드
- [ ] `rainfall.spec` 작성 (--onedir, --windowed, data/ 포함)
- [ ] `dist/RainFall/RainFall.exe` 빌드 및 실행 확인

### Phase 3: Inno Setup 설치 파일
- [ ] `installer.iss` 작성
- [ ] `RainFall_Setup_v2.0.exe` 산출 (내장 DB 포함)

### Phase 4: 자동 업데이트 기능
- [ ] `version.json` / `version_info.json` 구조 정의
- [ ] `updater.py` 신규 모듈 (앱/DB 버전 비교, DB zip 다운로드·배치)
- [ ] `main_app.py` 시작 시 `UpdateCheckThread` 연동

---

## 배포 후 기능 업데이트
- 결과 파일을 기반으로 확률강우량 분석
- 사용자 편의성 중심 UI 수정
