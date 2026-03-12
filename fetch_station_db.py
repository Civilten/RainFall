"""
WAMIS 전체 강우관측소 정보 수집 및 Excel DB 생성
- rf_dubrfobs : 전체 관측소 목록
- rf_obsinfo  : 관측소 제원 (좌표, 표고, 자료기간 등)

수정 이력:
  v2 - ConnectionResetError 재시도 처리 추가, 동시 스레드 축소(5→2),
       순차 요청 + 딜레이 방식으로 서버 부하 감소
"""

import urllib.request
import json
import time
import pandas as pd
from urllib.error import URLError, HTTPError

WAMIS_KEY = "bffb72855f69375f93bcb64cc09c6663b0b4c37f96"
OUT_PATH  = "wamis_station_db.xlsx"
HEADERS   = {"User-Agent": "Mozilla/5.0"}
TIMEOUT   = 20
MAX_RETRY = 5          # 재시도 횟수 증가
DELAY_BTW = 0.15       # 요청 간 딜레이 (초) - 서버 부하 분산


# ── 1. 전체 관측소 목록 수집 ──────────────────────────────────────────────────

def fetch_station_list() -> list:
    url = (
        "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_dubrfobs"
        "?key=" + WAMIS_KEY + "&output=json"
    )
    for attempt in range(MAX_RETRY):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
            if data.get("result", {}).get("code") == "success":
                return data.get("list", [])
        except Exception as e:
            print(f"  목록 수집 재시도 {attempt+1}/{MAX_RETRY}: {e}")
            time.sleep(2 * (attempt + 1))
    return []


# ── 2. 단일 관측소 제원 수집 (재시도 포함) ────────────────────────────────────

def fetch_obsinfo(obscd: str) -> dict:
    """
    수정: ConnectionResetError도 재시도 대상으로 변경.
    지수 백오프(0.5s → 1s → 2s → 4s → 8s) 적용.
    """
    url = (
        "http://www.wamis.go.kr:8080/wamis/openapi/wkw/rf_obsinfo"
        "?obscd=" + obscd + "&key=" + WAMIS_KEY + "&output=json"
    )
    for attempt in range(MAX_RETRY):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT) as r:
                data = json.loads(r.read().decode("utf-8"))
            if data.get("result", {}).get("code") == "success":
                items = data.get("list", [])
                return items[0] if items else {}
            return {}

        except HTTPError as e:
            # 4xx는 재시도해도 의미 없음
            if e.code < 500:
                return {}
            wait = 0.5 * (2 ** attempt)
            time.sleep(wait)

        except (ConnectionResetError, ConnectionAbortedError):
            # ← 핵심 수정: 즉시 리턴하지 않고 백오프 후 재시도
            wait = 0.5 * (2 ** attempt)
            time.sleep(wait)

        except (URLError, TimeoutError, OSError):
            wait = 0.5 * (2 ** attempt)
            time.sleep(wait)

        except Exception:
            time.sleep(1)

    return {}


# ── 3. 순차 수집 (딜레이 포함) ───────────────────────────────────────────────

def fetch_all_obsinfo(codes: list) -> dict:
    """
    수정: 동시 스레드 대신 순차 요청 + 딜레이.
    서버 연결 거부(ConnectionResetError)를 원천 차단.
    약 822 × 0.15s ≈ 2분 소요.
    """
    results = {}
    total = len(codes)

    for i, cd in enumerate(codes, 1):
        results[cd] = fetch_obsinfo(cd)
        time.sleep(DELAY_BTW)

        if i % 50 == 0 or i == total:
            success = sum(1 for v in results.values() if v)
            print(f"  진행: {i}/{total}  (성공: {success}개)")

    return results


# ── 4. 데이터 병합 ────────────────────────────────────────────────────────────

COLUMN_MAP = {
    "obscd":      "관측소코드",
    "obsnm":      "관측소명",
    "obsnmeng":   "영문명",
    "bbsnnm":     "수계명",
    "sbsncd":     "소유역코드",
    "mngorg":     "관리기관",
    "clsyn":      "폐쇄여부",
    "obsknd":     "관측종류",
    "opendt":     "개설일",
    "addr":       "주소",
    "lon":        "경도",
    "lat":        "위도",
    "shgt":       "표고(m)",
    "hrdtstart":  "시자료_시작",
    "hrdtend":    "시자료_종료",
    "dydtstart":  "일자료_시작",
    "dydtend":    "일자료_종료",
}

COL_ORDER = list(COLUMN_MAP.keys())



def decimal_to_dms(val: str) -> str:
    """소수점 좌표를 도-분-초 형식으로 변환. 이미 도분초 형식이면 그대로 반환."""
    s = str(val).strip()
    if not s:
        return s
    # 이미 도-분-초 형식 (예: 127-29-10)
    if "-" in s:
        return s
    try:
        deg_float = float(s)
        d = int(deg_float)
        m_float = (deg_float - d) * 60
        m = int(m_float)
        sec = round((m_float - m) * 60)
        # 반올림으로 초가 60이 될 경우 올림 처리
        if sec == 60:
            sec = 0
            m += 1
        if m == 60:
            m = 0
            d += 1
        return f"{d}-{m:02d}-{sec:02d}"
    except (ValueError, TypeError):
        return s


def build_dataframe(station_list: list, info_map: dict) -> pd.DataFrame:
    rows = []
    for stn in station_list:
        obscd = stn.get("obscd", "")
        info  = info_map.get(obscd, {})

        row = {
            "obscd":     obscd,
            "obsnm":     stn.get("obsnm",  info.get("obsnm",  "")),
            "obsnmeng":  info.get("obsnmeng", ""),
            "bbsnnm":    stn.get("bbsnnm", info.get("bbsnnm", "")),
            "sbsncd":    stn.get("sbsncd", info.get("sbsncd", "")),
            "mngorg":    stn.get("mngorg", info.get("mngorg", "")),
            "clsyn":     stn.get("clsyn",  ""),
            "obsknd":    stn.get("obsknd", info.get("obsknd", "")),
            "opendt":    info.get("opendt", ""),
            "addr":      info.get("addr",   ""),
            "lon":       decimal_to_dms(info.get("lon",    "")),
            "lat":       decimal_to_dms(info.get("lat",    "")),
            "shgt":      info.get("shgt",   ""),
            "hrdtstart": info.get("hrdtstart", ""),
            "hrdtend":   info.get("hrdtend",   ""),
            "dydtstart": info.get("dydtstart", ""),
            "dydtend":   info.get("dydtend",   ""),
        }
        rows.append(row)

    df = pd.DataFrame(rows, columns=COL_ORDER)
    df = df.sort_values(["bbsnnm", "obscd"]).reset_index(drop=True)
    df = df.rename(columns=COLUMN_MAP)
    return df


# ── 5. Excel 저장 ─────────────────────────────────────────────────────────────

def save_excel(df: pd.DataFrame, path: str):
    with pd.ExcelWriter(path, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="관측소DB")
        wb  = writer.book
        ws  = writer.sheets["관측소DB"]
        hdr = wb.add_format({
            "bold": True, "bg_color": "#1c9432",
            "font_color": "white", "border": 1, "align": "center",
        })
        for col_idx, col_name in enumerate(df.columns):
            ws.write(0, col_idx, col_name, hdr)
            col_data = df.iloc[:, col_idx].astype(str)
            max_len  = max(len(str(col_name)), col_data.str.len().max())
            ws.set_column(col_idx, col_idx, min(max_len + 2, 30))


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    print("[1/3] rf_dubrfobs 전체 관측소 목록 수집 중...")
    station_list = fetch_station_list()
    if not station_list:
        print("오류: 관측소 목록 수집 실패")
        return
    print(f"  -> {len(station_list)}개 관측소 확인")

    codes = [s["obscd"] for s in station_list if s.get("obscd")]
    codes = list(dict.fromkeys(codes))

    print(f"\n[2/3] rf_obsinfo 제원 수집 중... ({len(codes)}개, 순차 요청)")
    print(f"  예상 소요: 약 {len(codes) * DELAY_BTW / 60:.0f}~{len(codes) * (DELAY_BTW + 0.3) / 60:.0f}분")
    info_map = fetch_all_obsinfo(codes)
    success  = sum(1 for v in info_map.values() if v)
    print(f"  -> 완료: {success}/{len(codes)}개 제원 수집")

    print("\n[3/3] 데이터 병합 및 Excel 저장 중...")
    df = build_dataframe(station_list, info_map)
    save_excel(df, OUT_PATH)
    print(f"  -> 저장 완료: {OUT_PATH}  ({len(df)}행 x {len(df.columns)}열)")

    # 결과 통계
    print()
    has_lon = df["경도"].notna() & (df["경도"] != "")
    print(f"제원 보유: {has_lon.sum()}개 / 제원 없음: {(~has_lon).sum()}개")
    print(f"폐쇄여부 - 영업: {(df['폐쇄여부']=='영').sum()}개, 폐쇄: {(df['폐쇄여부']!='영').sum()}개")


if __name__ == "__main__":
    main()
