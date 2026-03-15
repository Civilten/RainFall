# 강우자료 통합 관리 및 분석 시스템 v2.0

## 프로젝트 개요
WAMIS 및 KMA API를 연동하여 강우 관측 데이터 수집, Parquet 기반의 고속 로컬 DB로 관리 및 엑셀(Excel)로 추출하는 PyQt6 데스크톱 통합 플랫폼.

## 기술 스택
- **Language:** Python 3.8+
- **GUI Framework:** PyQt6, qt-material (Custom Theme 적용)
- **Data Processing:** Pandas, Parquet (대용량 고속 I/O)
- **Export/Report:** Xlsxwriter
- **Concurrency:** QThread (비동기 UI 처리)

## 주요 파일 및 모듈 아키텍처
- `main_app.py`: 메인 GUI 애플리케이션 (관측소 목록 관리, 엑셀 추출, 관리자 모듈 UI)
- `api_fetcher.py`: WAMIS 및 기상청(KMA) API 연동, 시강우/일별/연최대 데이터 다운로드
- `max_rainfall_calculator.py`: 수집된 시강우를 바탕으로 고정시간 및 임의시간 최대강우량 자동 산출 및 환산
- `db_reviewer.py`: 구축된 DB 버전의 정합성 검토 및 요약 리포트(Excel) 생성
- `fetch_station_db.py`: WAMIS API를 통한 최신 관측소 제원(군집, 코드 등) 수집
- `convert_sta_to_parquet.py`: 엑셀 관측소 DB를 Parquet 포맷으로 변환

## 데이터베이스 구조 (DB Versions)
데이터는 `data/db_versions/{버전명}/` 경로에 6개의 핵심 Parquet 파일 세트로 관리됨.
1. `hourly.parquet`: 원시 시강우 데이터
2. `fixed_max.parquet`: 고정시간 최대강우량
3. `arb_max.parquet`: 임의시간 최대강우량
4. `kma_daily_max.parquet`: 기상청 일별 10분/60분 최대 자료 (특수 DB)
5. `kma_yearly_max.parquet`: 기상청 연최대치 데이터 (10분/1시간)
6. `arb_max_modified.parquet`: 임의시간 수정본 (환산계수 적용 및 기상청 1-HR 데이터 대체본)

## 현재 구현 상태 및 핵심 기능
- [완료] ui, db parquet 생성 기능
- [완료] 내장 DB 기반 엑셀 추출, DB 병합, DB 검토 요약 리포트, DB 편집
- [완료] 관측소 DB 생성
- [완료] API 실시간 다운로드 (날짜 범위 기반, 임시 DB 생성 후 엑셀 추출 및 자동 삭제)
- [버그/보류] KMA 관측소 API 다운로드 시 종료일=오늘 설정 → 빈 결과 반환
  - 원인: KMA API는 전일(T-1)까지만 데이터 제공, 당일 날짜 포함 청크 요청 시 에러 코드 반환 → 전체 청크 누락
  - 위치: `api_fetcher.py` `fetch_kma_hourly_rainfall` (L:144), `fetch_kma_daily_max_rainfall` (L:260)
  - 수정 방향: KMA 함수 내 end_date를 min(end_date, 어제)로 자동 보정
- [예정] 사용자 편의성 중심 UI 수정
- [예정] 프로그램 배포 준비 (docs/shareplan.md 참조)

## 상세 문서 (필요시 읽을 것)
- docs/PROJECT_OVERVIEW.md  → 프로젝트 개요
- docs/plan.md      → 현재 구현 계획
- docs/decisions.md → 설계 결정 기록