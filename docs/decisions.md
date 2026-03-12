# Architecture & Project Decisions

## 결정 내역 (Decision Log)

| 결정 항목 | 결정 내용 |
|----------|----------|
| 스토리지 포맷 | SQLite/CSV 대신 Parquet 채택 (대용량 고속 I/O) |
| API 청크 분할 | 연도별 상반기/하반기로 분할 호출 (대용량 응답 회피) |
| 비동기 처리 | concurrent.futures + QThread 조합 (UI 블로킹 방지) |
| 관리자 모드 UX | 타이틀 라벨 5회 클릭으로 활성화 (일반 사용자 접근 차단) |
| 역전보간 보정 | 긴 지속기간 강우량 < 짧은 지속기간 강우량 발생 시, 짧은 지속기간 값으로 대체 보정 |
| KMA 1-HR 대체 | `arb_max_modified`에서 WAMIS 1시간 대신 KMA 60분 최대값으로 대체 사용 |
| DB 버전 폴더명 | 미정 (임의 수정 가능) |
| 관측소 DB 분리 | `Sta1_db.parquet`(내장 DB 추출용) / `Sta2_db.parquet`(API 실시간 다운로드용) 별도 관리 |
| 탭 전환 시 관측소 목록 교체 | 탭마다 다른 관측소 DB 사용(Sta1/Sta2) → `_on_tab_changed`에서 `_load_station_table` 호출, 체크박스·검색창 전체 초기화 |
| API 다운로드 날짜 입력 | 연도 SpinBox → 연월일 QDateEdit (부분 연도 범위 다운로드 지원) |
| 추출 로직 분리 | `run_db_extraction`에서 핵심 로직을 `_do_extraction(db_path, ...)`으로 분리, 내장 DB 및 API 임시 DB 양쪽에서 재사용 |
