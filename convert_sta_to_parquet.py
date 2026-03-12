"""
관측소 DB 엑셀 → Parquet 변환 스크립트

변환할 엑셀 파일과 저장 위치를 대화창으로 선택합니다.

컬럼 (20개):
  관측소코드, 관측소명, 영문명, 수계명, 소유역코드, 관리기관,
  폐쇄여부, 관측종류, 개설일, 주소, 경도, 위도, 표고(m),
  시자료_시작, 시자료_종료, 일자료_시작, 일자료_종료,
  군집, 관측소코드-이전, 관측소명-이전

타입 정책:
  - 관측소코드 / 소유역코드 / 관측소코드-이전 : str (trailing .0 제거)
  - 경도 / 위도                            : str, 소수점이면 도-분-초 변환
  - 시/일자료 시작·종료                      : str YYYYMMDD (float→int→str 통일)
  - 표고(m)                               : float64
  - 군집                                  : Int64 (nullable 정수)
  - 나머지                                 : str
"""

import os
import tkinter
import tkinter.filedialog
import tkinter.messagebox
import pandas as pd


# ── 변환 헬퍼 ─────────────────────────────────────────────────────────────────

def decimal_to_dms(val) -> str:
    """소수점 좌표 → 도-분-초 변환. 이미 도분초 형식이거나 빈값이면 그대로."""
    s = str(val).strip() if pd.notna(val) else ""
    if not s or s == "nan":
        return ""
    if "-" in s:          # 이미 도-분-초 형식
        return s
    try:
        f = float(s)
        d = int(f)
        m_f = (f - d) * 60
        m = int(m_f)
        sec = round((m_f - m) * 60)
        if sec == 60: sec, m = 0, m + 1
        if m == 60:   m,   d = 0, d + 1
        return f"{d}-{m:02d}-{sec:02d}"
    except (ValueError, TypeError):
        return s


def to_str(val) -> str:
    """NaN/None → 빈 문자열, 그 외 strip 후 str 반환."""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    return "" if s == "nan" else s


def to_code_str(val) -> str:
    """정수형 코드(소수점 .0 포함) → 순수 숫자 문자열."""
    s = to_str(val)
    if s.endswith(".0"):
        s = s[:-2]
    return s


def to_date_str(val) -> str:
    """날짜를 YYYYMMDD 문자열로 통일. float(19830701.0), str 모두 처리."""
    if pd.isna(val):
        return ""
    s = str(val).strip()
    if s == "nan" or s == "":
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    return s


def to_nullable_int(val) -> "int | pd.NA":
    """군집 컬럼: 정수 또는 pd.NA."""
    if pd.isna(val):
        return pd.NA
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return pd.NA


# ── 컬럼 정의 (엑셀 헤더 순서 고정) ────────────────────────────────────────────

COLUMNS = [
    "관측소코드", "관측소명", "영문명", "수계명", "소유역코드",
    "관리기관", "폐쇄여부", "관측종류", "개설일", "주소",
    "경도", "위도", "표고(m)",
    "시자료_시작", "시자료_종료", "일자료_시작", "일자료_종료",
    "군집", "관측소코드-이전", "관측소명-이전",
]


# ── 변환 메인 ─────────────────────────────────────────────────────────────────

def convert(xlsx_path: str, parquet_path: str):
    xlsx_name    = os.path.basename(xlsx_path)
    parquet_name = os.path.basename(parquet_path)

    print(f"[읽기] {xlsx_name}")
    df_raw = pd.read_excel(xlsx_path)

    # 헤더 수 검증
    if len(df_raw.columns) != 20:
        print(f"  ⚠ 컬럼 수 불일치: {len(df_raw.columns)}개 (예상 20개)")
        print(f"     실제 컬럼: {list(df_raw.columns)}")
        return False

    # 컬럼명 강제 매핑 (순서 기반)
    df_raw.columns = COLUMNS

    print(f"  원본: {len(df_raw)}행 × {len(df_raw.columns)}열")

    # ── 타입별 변환 ──
    df = pd.DataFrame()

    df["관측소코드"]      = df_raw["관측소코드"].apply(to_code_str)
    df["관측소명"]       = df_raw["관측소명"].apply(to_str)
    df["영문명"]         = df_raw["영문명"].apply(to_str)
    df["수계명"]         = df_raw["수계명"].apply(to_str)
    df["소유역코드"]      = df_raw["소유역코드"].apply(to_code_str)
    df["관리기관"]       = df_raw["관리기관"].apply(to_str)
    df["폐쇄여부"]       = df_raw["폐쇄여부"].apply(to_str)
    df["관측종류"]       = df_raw["관측종류"].apply(to_str)
    df["개설일"]         = df_raw["개설일"].apply(to_str)
    df["주소"]           = df_raw["주소"].apply(to_str)
    df["경도"]           = df_raw["경도"].apply(decimal_to_dms)
    df["위도"]           = df_raw["위도"].apply(decimal_to_dms)
    df["표고(m)"]        = pd.to_numeric(df_raw["표고(m)"], errors="coerce")
    df["시자료_시작"]     = df_raw["시자료_시작"].apply(to_date_str)
    df["시자료_종료"]     = df_raw["시자료_종료"].apply(to_date_str)
    df["일자료_시작"]     = df_raw["일자료_시작"].apply(to_date_str)
    df["일자료_종료"]     = df_raw["일자료_종료"].apply(to_date_str)
    df["군집"]           = pd.array(
                              [to_nullable_int(v) for v in df_raw["군집"]],
                              dtype=pd.Int64Dtype()
                           )
    df["관측소코드-이전"]  = df_raw["관측소코드-이전"].apply(to_code_str)
    df["관측소명-이전"]   = df_raw["관측소명-이전"].apply(to_str)

    # ── 데이터 누락 검증 ──
    assert len(df) == len(df_raw), "행 수 불일치!"
    assert list(df.columns) == COLUMNS, "컬럼 순서 불일치!"

    # ── 저장 ──
    df.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"  저장: {parquet_name}  ({len(df)}행 × {len(df.columns)}열)")

    # ── 검증 리드백 ──
    df_check = pd.read_parquet(parquet_path)
    assert len(df_check) == len(df), "저장 후 행 수 불일치!"
    print(f"  검증: OK  (재로드 후 {len(df_check)}행 확인)")

    # ── 샘플 출력 ──
    print(f"  dtypes:")
    for col, dtype in df.dtypes.items():
        sample = df[col].dropna().iloc[0] if df[col].notna().any() else None
        print(f"    {col:<20} {str(dtype):<12} 예={repr(sample)}")
    print()
    return True


def main():
    print("=" * 60)
    print("관측소 DB Excel → Parquet 변환")
    print("=" * 60)

    # tkinter 숨김 루트 생성
    root = tkinter.Tk()
    root.withdraw()
    root.attributes("-topmost", True)

    # ── 입력 xlsx 선택 ──
    xlsx_path = tkinter.filedialog.askopenfilename(
        parent=root,
        title="변환할 엑셀 파일을 선택하세요",
        filetypes=[("Excel 파일", "*.xlsx"), ("모든 파일", "*.*")],
    )
    if not xlsx_path:
        print("취소됨.")
        root.destroy()
        return

    # ── 출력 parquet 경로 선택 ──
    default_stem = os.path.splitext(os.path.basename(xlsx_path))[0]
    default_dir  = os.path.dirname(xlsx_path)
    parquet_path = tkinter.filedialog.asksaveasfilename(
        parent=root,
        title="저장할 Parquet 파일 경로를 지정하세요",
        initialdir=default_dir,
        initialfile=default_stem + ".parquet",
        defaultextension=".parquet",
        filetypes=[("Parquet 파일", "*.parquet"), ("모든 파일", "*.*")],
    )
    if not parquet_path:
        print("취소됨.")
        root.destroy()
        return

    # ── 변환 실행 ──
    success = convert(xlsx_path, parquet_path)

    if success:
        print("완료.")
        tkinter.messagebox.showinfo(
            "완료",
            f"변환이 완료되었습니다.\n저장 위치: {parquet_path}",
            parent=root,
        )
    else:
        tkinter.messagebox.showerror(
            "실패",
            "변환 중 오류가 발생했습니다. 터미널 로그를 확인하세요.",
            parent=root,
        )

    root.destroy()


if __name__ == "__main__":
    main()
