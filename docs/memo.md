# plan

## 단기 목표 ✅ 완료 (2026-03-12)
- [x] API 실시간 다운로드 기능 구현 (구축된 내장 DB 기반 엑셀 추출과는 별개의 기능)
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

## 중장기 로드맵
### 사용자 편의성 중심 UI 수정
### 프로그램 배포
- 내장 DB를 설치 파일에 포함
- 내장 DB 인터넷으로 업데이트 방안 모색

## 배포 후 기능 업데이트
### 결과 파일을 기반으로 확률강우량 분석
