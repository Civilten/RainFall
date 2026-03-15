# RainFall 배포 계획 v2.0

## 개요

RainFall 프로그램을 Python 미설치 일반 사용자 환경에 배포하기 위한 단계별 계획.

---

## Phase 1: 빌드 환경 세팅

### `requirements.txt` 생성
```
PyQt6>=6.4.0
qt-material>=2.14
pandas>=1.5.0
numpy>=1.23.0
xlsxwriter>=3.0.0
pyinstaller>=6.0.0
```

---

## Phase 2: PyInstaller EXE 빌드

### `rainfall.spec` 생성 (PyInstaller 빌드 설정)

- **진입점:** `main_app.py`
- **빌드 방식:** `--onedir` (단일 디렉터리)
- **포함 데이터:**
  - `data/` 전체 (Parquet DB, config.json)
  - `custom_theme.xml`
- **옵션:** `--windowed` (콘솔창 없음)
- **출력:** `dist/RainFall/RainFall.exe`

```bash
pyinstaller rainfall.spec
```

---

## Phase 3: Inno Setup 설치 파일

### `installer.iss` 생성

- `dist/RainFall/` 전체를 설치 파일에 패키징
- 설치 경로: `%ProgramFiles%\RainFall`
- 바탕화면 / 시작 메뉴 바로가기 생성
- 언인스톨러 포함

**최종 산출물:** `RainFall_Setup_v2.0.exe` — 내장 DB 포함 단일 설치 파일

---

## Phase 4: 업데이트 기능

### 버전 관리 파일

**`version.json`** (프로그램 설치 폴더에 포함)
```json
{
  "app_version": "2.0.0",
  "db_version": "~2025"
}
```

**`version_info.json`** (GitHub Releases에 업로드 — 항상 최신 버전 정보)
```json
{
  "app_version": "2.0.1",
  "app_download_url": "https://github.com/.../releases/download/v2.0.1/RainFall_Setup_v2.0.1.exe",
  "db_version": "~2026",
  "db_download_url": "https://github.com/.../releases/download/db-~2026/db_~2026.zip",
  "db_changelog": "2026년 자료 추가"
}
```

### `updater.py` 신규 모듈

| 함수 | 역할 |
|------|------|
| `check_app_update()` | GitHub에서 `version_info.json` fetch → 버전 비교 |
| `check_db_update()` | DB 버전 비교 |
| `download_db_update(url, dest)` | DB zip 다운로드 → 압축 해제 → `data/db_versions/` 배치 → `config.json` 업데이트 |
| `UpdateCheckThread` | QThread 기반 비동기 처리 |

### `main_app.py` 수정 사항

- 앱 시작 시 `UpdateCheckThread` 실행
- **프로그램 업데이트 감지:** 알림 다이얼로그 → "다운로드 페이지 열기" 버튼 (`QDesktopServices.openUrl`)
- **DB 업데이트 감지:** 알림 다이얼로그 → "지금 업데이트" 버튼 → 진행 바 표시 → 완료 후 재시작 안내

---

## GitHub Releases 운영 방식

| 릴리즈 유형 | 태그 예시 | 첨부 파일 |
|------------|---------|---------|
| 프로그램 업데이트 | `v2.0.1` | `RainFall_Setup_v2.0.1.exe` |
| DB 업데이트 | `db-~2026` | `db_~2026.zip` (parquet 6개) |

- `version_info.json`은 raw URL로 항상 최신 버전 접근 가능하게 유지

---

## 신규/수정 파일 목록

| 파일 | 상태 | 역할 |
|------|------|------|
| `requirements.txt` | 신규 | 의존성 버전 고정 |
| `rainfall.spec` | 신규 | PyInstaller 빌드 설정 |
| `installer.iss` | 신규 | Inno Setup 설치 파일 생성 |
| `version.json` | 신규 | 현재 앱/DB 버전 정보 |
| `updater.py` | 신규 | 업데이트 확인 및 다운로드 로직 |
| `main_app.py` | 수정 | 시작 시 업데이트 체크 연동 |

---

## 검증 방법

1. `pyinstaller rainfall.spec` → `dist/RainFall/RainFall.exe` 직접 실행 확인
2. Inno Setup Compiler로 `installer.iss` 컴파일 → 설치 후 실행 확인
3. `version_info.json`에 상위 버전 기재 후 앱 실행 → 업데이트 알림 표시 확인
4. DB 업데이트 다운로드 → `data/db_versions/` 적용 → 정상 로드 확인
