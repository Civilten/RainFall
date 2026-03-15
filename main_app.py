import sys
import os
import pandas as pd
from datetime import datetime
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QTabWidget, QPushButton, QTableWidget,
                             QTableWidgetItem, QCheckBox, QLabel, QLineEdit,
                             QTextEdit, QProgressBar, QSpinBox, QGroupBox, QMessageBox,
                             QComboBox, QHeaderView, QFileDialog, QTableView, QDateEdit)
from PyQt6.QtCore import Qt, QEvent, QTimer, QThread, pyqtSignal, QAbstractTableModel, QDate
import shutil
import json
from qt_material import apply_stylesheet

from api_fetcher import fetch_wamis_hourly_rainfall, fetch_kma_hourly_rainfall, fetch_kma_daily_max_rainfall
from db_reviewer import generate_db_review_report
from max_rainfall_calculator import process_hourly_to_max, convert_to_arbitrary_max_with_kma_yearly
from fetch_station_db import fetch_obsinfo, decimal_to_dms

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_DIR = os.path.join(BASE_DIR, "data", "db_versions")
TEMP_DIR = os.path.join(BASE_DIR, "data", "temp")
CONFIG_PATH = os.path.join(BASE_DIR, "data", "config.json")
MASTER_EXCEL_PATH = os.path.join(BASE_DIR, "data", "강우관측소(지역빈도).xlsx")

# 개발자/버전 정보 상수
APP_VERSION = "2.0"
APP_AUTHOR = "김찬영"
APP_DATE = "2023.11.28. (개정 2026.02)"

def _make_output_subdir(base_dir: str) -> str:
    """base_dir 아래에 강우YYMMDD_N 폴더를 생성 후 경로 반환. 중복 시 N 증가."""
    date_str = datetime.now().strftime("%y%m%d")
    n = 1
    while True:
        folder_name = f"강우{date_str}_{n}"
        full_path = os.path.join(base_dir, folder_name)
        if not os.path.exists(full_path):
            os.makedirs(full_path)
            return full_path
        n += 1


class PandasModel(QAbstractTableModel):
    def __init__(self, data):
        super().__init__()
        self._data = data

    def rowCount(self, parent=None):
        if self._data is None: return 0
        return self._data.shape[0]

    def columnCount(self, parent=None):
        if self._data is None: return 0
        return self._data.shape[1]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if index.isValid():
            if role == Qt.ItemDataRole.DisplayRole or role == Qt.ItemDataRole.EditRole:
                val = self._data.iat[index.row(), index.column()]
                return str(val) if pd.notna(val) else ""
        return None

    def setData(self, index, value, role):
        if role == Qt.ItemDataRole.EditRole:
            try:
                col_type = self._data.dtypes.iloc[index.column()]
                if pd.api.types.is_numeric_dtype(col_type):
                    self._data.iat[index.row(), index.column()] = float(value) if value else 0.0
                else:
                    self._data.iat[index.row(), index.column()] = value
                self.dataChanged.emit(index, index, [Qt.ItemDataRole.DisplayRole])
                return True
            except Exception:
                return False
        return False

    def headerData(self, col, orientation, role):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return str(self._data.columns[col])
        return None

    def flags(self, index):
        return super().flags(index) | Qt.ItemFlag.ItemIsEditable

class ApiDownloadThread(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(bool, str)

    TARGET_ORG_KMA = "기상청"

    def __init__(self, stations, start_yr, end_yr, new_version_name,
                 kma_only=False, save_path=None, start_date=None, end_date=None):
        super().__init__()
        self.stations = stations
        self.start_yr = start_yr
        self.end_yr = end_yr
        self.new_version_name = new_version_name
        self.kma_only = kma_only
        self.save_path = save_path
        self.start_date = start_date
        self.end_date = end_date

    def run(self):
        self.log_signal.emit(f"DATA 다운로드 시작: {self.start_yr}~{self.end_yr}년 ({len(self.stations)}개 관측소)")
        
        all_new_data = []
        all_kma_daily_data = []
        total = len(self.stations)
        
        for i, stn in enumerate(self.stations):
            code = stn['code']
            name = stn.get('name', '알수없음')
            org = stn.get('org', '')
            
            is_kma_station = self.TARGET_ORG_KMA in org
            api_target = self.TARGET_ORG_KMA if is_kma_station else "WAMIS"
            self.log_signal.emit(f"({i+1}/{total}) [{name}] DATA 다운로드 중... ({api_target})")
            
            try:
                if self.kma_only:
                    if not is_kma_station:
                        self.log_signal.emit(f"  -> {self.TARGET_ORG_KMA} 관할이 아니므로 건너뜀")
                        continue
                    
                    daily_df = fetch_kma_daily_max_rainfall(code, self.start_yr, self.end_yr, self.start_date, self.end_date)
                    if not daily_df.empty:
                        all_kma_daily_data.append(daily_df)
                    continue

                if is_kma_station:
                    df = fetch_kma_hourly_rainfall(code, self.start_yr, self.end_yr, self.start_date, self.end_date)

                    # 4번째 DB: 기상청 일별 10분/60분 최대 처리
                    daily_df = fetch_kma_daily_max_rainfall(code, self.start_yr, self.end_yr, self.start_date, self.end_date)
                    if not daily_df.empty:
                        all_kma_daily_data.append(daily_df)

                else:
                    df = fetch_wamis_hourly_rainfall(code, self.start_yr, self.end_yr, self.start_date, self.end_date)
                
                if not df.empty:
                    all_new_data.append(df)
                    
            except Exception as e:
                self.log_signal.emit(f"  -> 오류 발생: {e}")
                
            progress = int(((i + 1) / total) * 100)
            self.progress_signal.emit(progress)
            
        if self.kma_only:
             if not all_kma_daily_data:
                 self.finished_signal.emit(False, f"수집된 {self.TARGET_ORG_KMA} 특수 데이터가 없습니다.")
                 return
             
             self.log_signal.emit(f"다운로드 완료. 신규 특수 DB 버전 [{self.new_version_name}] 세트 생성 중...")
             target_dir = self.save_path if self.save_path else os.path.join(DB_DIR, self.new_version_name)
             os.makedirs(target_dir, exist_ok=True)
             daily_path = os.path.join(target_dir, "kma_daily_max.parquet")
             yearly_path = os.path.join(target_dir, "kma_yearly_max.parquet")
             
             new_daily_df = pd.concat(all_kma_daily_data, ignore_index=True)
             new_daily_df.to_parquet(daily_path, index=False)
             daily_count = len(new_daily_df)
             
             df_yearly = new_daily_df.groupby(['STN_CD', 'YEAR'])[['MI10_MAX_RN', 'HR1_MAX_RN']].max().reset_index()
             df_yearly.to_parquet(yearly_path, index=False)
             
             test_excel_path = f"kma_daily_max_test_{self.new_version_name}.xlsx"
             new_daily_df.to_excel(test_excel_path, index=False)
             
             self.finished_signal.emit(True, f"새로운 특수 DB 버전 [{self.new_version_name}] 저장 완료!\n- 일/연최대: {daily_count}건")
             return

        if not all_new_data:
            self.finished_signal.emit(False, "수집된 새로운 데이터가 없습니다.")
            return
            
        self.log_signal.emit(f"다운로드 완료. Data 정리중...")
        try:
            new_df = pd.concat(all_new_data, ignore_index=True)

            target_dir = self.save_path if self.save_path else os.path.join(DB_DIR, self.new_version_name)
            os.makedirs(target_dir, exist_ok=True)
            
            hourly_path = os.path.join(target_dir, "hourly.parquet")
            fixed_path = os.path.join(target_dir, "fixed_max.parquet")
            arb_path = os.path.join(target_dir, "arb_max.parquet")
            
            # 1. 시강우 Parquet 저장
            new_df.to_parquet(hourly_path, index=False)
            
            self.log_signal.emit(f"최대강우량(고정/임의) 산출 중...")
            
            # 2. & 3. 고정시간 및 임의시간 최대강우량 산출 및 저장
            df_fixed, df_arb = process_hourly_to_max(new_df)
            
            df_fixed.to_parquet(fixed_path, index=False)
            df_arb.to_parquet(arb_path, index=False)
            
            # 4. 기상청 일별 10/60분 최대 자료 저장
            daily_path = os.path.join(target_dir, "kma_daily_max.parquet")
            yearly_path = os.path.join(target_dir, "kma_yearly_max.parquet")
            daily_count = 0
            if all_kma_daily_data:
                self.log_signal.emit(f"기상청 10/60분 최대 강우 산출 중...")
                new_daily_df = pd.concat(all_kma_daily_data, ignore_index=True)
                new_daily_df.to_parquet(daily_path, index=False)
                daily_count = len(new_daily_df)

                # 5. 연최대치 데이터프레임 추출 (10분/1시간 각각 최대값 산출)
                df_yearly = new_daily_df.groupby(['STN_CD', 'YEAR'])[['MI10_MAX_RN', 'HR1_MAX_RN']].max().reset_index()
                df_yearly.to_parquet(yearly_path, index=False)
            
            # 6. 임의시간 수정본 (arb_max_modified) 산출: df_fixed 원본에 환산계수를 곱하고 1-HR만 기상청 연최대치로 대체
            mod_arb_path = os.path.join(target_dir, "arb_max_modified.parquet")
            if all_kma_daily_data and not df_yearly.empty:
                df_arb_mod = convert_to_arbitrary_max_with_kma_yearly(df_fixed, df_yearly)
            else:
                df_arb_mod = df_arb.copy() # 기상청 자료가 한 건도 없으면 그냥 기존 arb_max와 동일하게 저장
                
            df_arb_mod.to_parquet(mod_arb_path, index=False)
            
            self.finished_signal.emit(True, f"새로운 DB 버전 [{self.new_version_name}] 저장 완료!\n- 시강우: {len(new_df)}건\n- 최대강우(고정/임의/수정임의): {len(df_fixed)}건\n- 기상청(일자료/연자료): {daily_count}건")
        except Exception as e:
            self.finished_signal.emit(False, f"DB 저장 중 오류: {e}")



class FetchStationThread(QThread):
    """fetch_station_db.py 실행 → wamis_station_db.xlsx + 이전 헤더 컬럼 추가"""
    log_signal      = pyqtSignal(str)
    finished_signal = pyqtSignal(bool)

    def __init__(self, base_dir):
        super().__init__()
        self.base_dir = base_dir

    def run(self):
        import subprocess, shutil
        script = os.path.join(self.base_dir, "fetch_station_db.py")
        try:
            proc = subprocess.Popen(
                [sys.executable, script],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                cwd=self.base_dir, text=True, encoding="utf-8", errors="replace",
            )
            for line in proc.stdout:
                self.log_signal.emit(line.rstrip())
            proc.wait()
            if proc.returncode != 0:
                self.log_signal.emit(f"스크립트 종료 코드: {proc.returncode}")
                self.finished_signal.emit(False)
                return

            # wamis_station_db.xlsx 에 군집 헤더 컬럼 추가
            out_path = os.path.join(self.base_dir, "wamis_station_db.xlsx")
            df = pd.read_excel(out_path)
            df["군집"] = ""
            with pd.ExcelWriter(out_path, engine="xlsxwriter") as writer:
                df.to_excel(writer, index=False, sheet_name="관측소DB")
                wb  = writer.book
                ws  = writer.sheets["관측소DB"]
                hdr = wb.add_format({
                    "bold": True, "bg_color": "#1c9432",
                    "font_color": "white", "border": 1, "align": "center",
                })
                for ci, cn in enumerate(df.columns):
                    ws.write(0, ci, cn, hdr)
            self.log_signal.emit("✓ 헤더 컬럼 추가 완료: 군집")
            self.finished_signal.emit(True)
        except Exception as e:
            self.log_signal.emit(f"오류: {e}")
            self.finished_signal.emit(False)


class ConvertStaThread(QThread):
    """convert_sta_to_parquet.py 실행 (파일 선택 대화창은 스크립트 내부에서 처리)"""
    log_signal      = pyqtSignal(str)
    finished_signal = pyqtSignal(bool)

    def __init__(self, base_dir):
        super().__init__()
        self.base_dir = base_dir

    def run(self):
        import subprocess
        self.cancelled = False
        script = os.path.join(self.base_dir, "convert_sta_to_parquet.py")
        try:
            proc = subprocess.Popen(
                [sys.executable, script],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                cwd=self.base_dir, text=True, encoding="utf-8", errors="replace",
            )
            for line in proc.stdout:
                self.log_signal.emit(line.rstrip())
            proc.wait()
            if proc.returncode == 2:  # 사용자 취소
                self.cancelled = True
                self.finished_signal.emit(False)
                return
            if proc.returncode != 0:
                self.log_signal.emit(f"스크립트 종료 코드: {proc.returncode}")
                self.finished_signal.emit(False)
                return
            self.finished_signal.emit(True)
        except Exception as e:
            self.log_signal.emit(f"오류: {e}")
            self.finished_signal.emit(False)


class ExtractionThread(QThread):
    log_signal      = pyqtSignal(str)
    progress_signal = pyqtSignal(int)
    finished_signal = pyqtSignal(bool, str)

    def __init__(self, db_path, save_dir, start_yr, end_yr, selected_stations, sta_parquet_path):
        super().__init__()
        self.db_path = db_path
        self.save_dir = save_dir
        self.start_yr = start_yr
        self.end_yr = end_yr
        self.selected_stations = selected_stations
        self.sta_parquet_path = sta_parquet_path

    def run(self):
        import xlsxwriter
        selected_stations = self.selected_stations
        db_path = self.db_path
        save_dir = self.save_dir
        start_yr = self.start_yr
        end_yr = self.end_yr

        self.log_signal.emit(f"총 {len(selected_stations)}개 관측소의 {start_yr}~{end_yr}년 데이터를 추출합니다...")

        # 군집별로 분리
        clusters_dict = {}
        for stn in selected_stations:
            c = stn['cluster'] if stn['cluster'] != '-' else '기타'
            if c not in clusters_dict:
                clusters_dict[c] = []
            clusters_dict[c].append(stn['code'])

        # Parquet DB 로딩
        try:
            df_hourly = pd.read_parquet(os.path.join(db_path, 'hourly.parquet'))
            df_hourly_original = df_hourly.copy()
            df_fixed = pd.read_parquet(os.path.join(db_path, 'fixed_max.parquet'))
            df_arb = pd.read_parquet(os.path.join(db_path, 'arb_max.parquet'))

            kma_daily_path = os.path.join(db_path, 'kma_daily_max.parquet')
            df_kma_daily = pd.read_parquet(kma_daily_path) if os.path.exists(kma_daily_path) else pd.DataFrame()

            kma_yearly_path = os.path.join(db_path, 'kma_yearly_max.parquet')
            df_kma_yearly = pd.read_parquet(kma_yearly_path) if os.path.exists(kma_yearly_path) else pd.DataFrame()

            arb_mod_path = os.path.join(db_path, 'arb_max_modified.parquet')
            df_arb_mod = pd.read_parquet(arb_mod_path) if os.path.exists(arb_mod_path) else pd.DataFrame()
        except Exception as e:
            self.log_signal.emit(f"⚠️ 데이터베이스 로딩 실패: {e}\n(해당 버전 폴더 내 3가지 parquet 파일이 온전한지 확인해주세요.)")
            self.finished_signal.emit(False, str(e))
            return

        # 데이터 연도 필터링
        df_hourly = df_hourly[(df_hourly['YEAR'] >= start_yr) & (df_hourly['YEAR'] <= end_yr)]
        df_fixed = df_fixed[(df_fixed['YEAR'] >= start_yr) & (df_fixed['YEAR'] <= end_yr)]
        df_arb = df_arb[(df_arb['YEAR'] >= start_yr) & (df_arb['YEAR'] <= end_yr)]
        if not df_kma_daily.empty:
            df_kma_daily = df_kma_daily[(df_kma_daily['YEAR'] >= start_yr) & (df_kma_daily['YEAR'] <= end_yr)]
        if not df_kma_yearly.empty:
            df_kma_yearly = df_kma_yearly[(df_kma_yearly['YEAR'] >= start_yr) & (df_kma_yearly['YEAR'] <= end_yr)]
        if not df_arb_mod.empty:
            df_arb_mod = df_arb_mod[(df_arb_mod['YEAR'] >= start_yr) & (df_arb_mod['YEAR'] <= end_yr)]

        # 군집별 폴더 생성 및 엑셀 저장
        total_stations = len(selected_stations)
        total_clusters = len(clusters_dict)
        current_cluster = 0
        processed_stations = 0

        for cluster_name, codes in clusters_dict.items():
            current_cluster += 1
            folder_label = "기타 폴더" if cluster_name == '기타' else f"{cluster_name}군집 폴더"
            self.log_signal.emit(f"[{current_cluster}/{total_clusters}] {folder_label} 생성 및 엑셀 작성 중...")

            if cluster_name == '기타':
                c_dir = os.path.join(save_dir, "기타")
            else:
                c_dir = os.path.join(save_dir, f"{cluster_name}군집")
            os.makedirs(c_dir, exist_ok=True)

            df_h_filtered = df_hourly[df_hourly['STN_CD'].isin(codes)]
            df_f_filtered = df_fixed[df_fixed['STN_CD'].isin(codes)]
            df_a_filtered = df_arb[df_arb['STN_CD'].isin(codes)]

            # 1. 시강우.xlsx 생성
            with pd.ExcelWriter(os.path.join(c_dir, "1. 시강우.xlsx"), engine='xlsxwriter') as writer:
                for code in sorted(codes):
                    stn_data = df_h_filtered[df_h_filtered['STN_CD'] == code].copy()
                    if stn_data.empty:
                        pd.DataFrame(columns=['연월일'] + [f'{i}시' for i in range(1, 25)]).to_excel(writer, sheet_name=str(code), index=False)
                    else:
                        stn_data['연월일'] = stn_data.apply(
                            lambda row: f"{int(row['YEAR'])}-{int(row['MONTH']):02d}-{int(row['DAY']):02d}"
                            if pd.notna(row['YEAR']) and pd.notna(row['MONTH']) and pd.notna(row['DAY']) else "", axis=1
                        )
                        cols = ['연월일'] + [f'H{i}' for i in range(1, 25)]
                        output_df = stn_data[cols].copy()
                        output_df.columns = ['연월일'] + [f'{i}시' for i in range(1, 25)]
                        output_df.to_excel(writer, sheet_name=str(code), index=False)

            # 2. 고정시간최대강우.xlsx 생성
            with pd.ExcelWriter(os.path.join(c_dir, "2. 고정시간최대강우.xlsx"), engine='xlsxwriter') as writer:
                for code in sorted(codes):
                    stn_data = df_f_filtered[df_f_filtered['STN_CD'] == code].copy()
                    if stn_data.empty:
                        pd.DataFrame(columns=[''] + [i for i in range(1, 73)]).to_excel(writer, sheet_name=str(code), index=False)
                    else:
                        stn_data.rename(columns={'YEAR': '연도'}, inplace=True)
                        cols = ['연도'] + [f'{i}-HR' for i in range(1, 73)]
                        output_df = stn_data[cols].copy()
                        output_df.columns = [''] + [i for i in range(1, 73)]
                        output_df.to_excel(writer, sheet_name=str(code), index=False)

            # 3. 임의시간최대강우.xlsx 생성
            with pd.ExcelWriter(os.path.join(c_dir, "3. 임의시간최대강우.xlsx"), engine='xlsxwriter') as writer:
                for code in sorted(codes):
                    stn_data = df_a_filtered[df_a_filtered['STN_CD'] == code].copy()
                    if stn_data.empty:
                        pd.DataFrame(columns=[''] + [i for i in range(1, 73)]).to_excel(writer, sheet_name=str(code), index=False)
                    else:
                        stn_data.rename(columns={'YEAR': '연도'}, inplace=True)
                        cols = ['연도'] + [f'{i}-HR' for i in range(1, 73)]
                        output_df = stn_data[cols].copy()
                        output_df.columns = [''] + [i for i in range(1, 73)]
                        output_df.to_excel(writer, sheet_name=str(code), index=False)

            # 1.1 기상청10_60min 강우.xlsx 생성
            if not df_kma_daily.empty:
                df_k_filtered = df_kma_daily[df_kma_daily['STN_CD'].isin(codes)]
                if not df_k_filtered.empty:
                    with pd.ExcelWriter(os.path.join(c_dir, "1.1 기상청10_60min 강우.xlsx"), engine='xlsxwriter') as writer:
                        for code in sorted(codes):
                            stn_data = df_k_filtered[df_k_filtered['STN_CD'] == code].copy()
                            if stn_data.empty:
                                pd.DataFrame(columns=['연월일', '10분 최다강수량', '1시간 최다강수량']).to_excel(writer, sheet_name=str(code), index=False)
                            else:
                                stn_data['연월일'] = stn_data.apply(
                                    lambda row: f"{int(row['YEAR'])}-{int(row['MONTH']):02d}-{int(row['DAY']):02d}"
                                    if pd.notna(row['YEAR']) and pd.notna(row['MONTH']) and pd.notna(row['DAY']) else "", axis=1
                                )
                                output_df = stn_data[['연월일', 'MI10_MAX_RN', 'HR1_MAX_RN']].copy()
                                output_df.columns = ['연월일', '10분 최다강수량', '1시간 최다강수량']
                                output_df.to_excel(writer, sheet_name=str(code), index=False)

            # 2.1 기상청10_60min 연최대강우.xlsx 생성
            if not df_kma_yearly.empty:
                df_y_filtered = df_kma_yearly[df_kma_yearly['STN_CD'].isin(codes)]
                if not df_y_filtered.empty:
                    with pd.ExcelWriter(os.path.join(c_dir, "2.1 기상청10_60min 연최대강우.xlsx"), engine='xlsxwriter') as writer:
                        for code in sorted(codes):
                            stn_data = df_y_filtered[df_y_filtered['STN_CD'] == code].copy()
                            if stn_data.empty:
                                pd.DataFrame(columns=['연도', '10분 최다강수량', '1시간 최다강수량']).to_excel(writer, sheet_name=str(code), index=False)
                            else:
                                output_df = stn_data[['YEAR', 'MI10_MAX_RN', 'HR1_MAX_RN']].copy()
                                output_df.columns = ['연도', '10분 최다강수량', '1시간 최다강수량']
                                output_df.to_excel(writer, sheet_name=str(code), index=False)

            # 3.1 임의시간최대강우_기상청60min적용.xlsx 생성
            if not df_kma_daily.empty:
                df_k_filtered = df_kma_daily[df_kma_daily['STN_CD'].isin(codes)]
                if not df_k_filtered.empty:
                    df_m_filtered = df_arb_mod[df_arb_mod['STN_CD'].isin(codes)]
                    if not df_m_filtered.empty:
                        with pd.ExcelWriter(os.path.join(c_dir, "3.1 임의시간최대강우_기상청60min적용.xlsx"), engine='xlsxwriter') as writer:
                            for code in sorted(codes):
                                stn_data = df_m_filtered[df_m_filtered['STN_CD'] == code].copy()
                                if stn_data.empty:
                                    pd.DataFrame(columns=[''] + [i for i in range(1, 73)]).to_excel(writer, sheet_name=str(code), index=False)
                                else:
                                    stn_data.rename(columns={'YEAR': '연도'}, inplace=True)
                                    cols = ['연도'] + [f'{i}-HR' for i in range(1, 73)]
                                    output_df = stn_data[cols].copy()
                                    output_df.columns = [''] + [i for i in range(1, 73)]
                                    output_df.to_excel(writer, sheet_name=str(code), index=False)

            processed_stations += len(codes)
            self.progress_signal.emit(int(processed_stations / total_stations * 90))

        # 관측소 제원 엑셀 생성
        try:
            sta_df = pd.read_parquet(self.sta_parquet_path)
            selected_codes = [stn['code'] for stn in selected_stations]
            _write_station_info_excel(save_dir, sta_df, df_hourly_original, selected_codes)
            self.log_signal.emit("✅ 0. 관측소제원.xlsx 저장 완료")
        except Exception as e:
            self.log_signal.emit(f"⚠️ 관측소제원.xlsx 생성 실패: {e}")
        self.progress_signal.emit(100)

        self.log_signal.emit(f"✅ 추출 완료! ({save_dir}에 결과 저장됨)")
        self.finished_signal.emit(True, save_dir)


def _write_station_info_excel(save_dir, sta_df, df_hourly_original, selected_codes):
    import xlsxwriter

    # 고정 14컬럼 스펙: (출력헤더, parquet컬럼명, API필드)
    FIXED_SPEC = [
        ("관측소코드",  "관측소코드",  None),
        ("관측소명",    "관측소명",    "obsnm"),
        ("수계명",      "수계명",      "bbsnnm"),
        ("소유역코드",  "소유역코드",  "sbsncd"),
        ("관리기관",    "관리기관",    "mngorg"),
        ("폐쇄여부",    "폐쇄여부",    "clsyn"),
        ("관측종류",    "관측종류",    "obsknd"),
        ("개설일",      "개설일",      "opendt"),
        ("주소",        "주소",        "addr"),
        ("경도",        "경도",        "lon"),
        ("위도",        "위도",        "lat"),
        ("표고",        "표고(m)",     "shgt"),
        ("시자료_시작", "시자료_시작", "hrdtstart"),
        ("시자료_종료", "시자료_종료", "hrdtend"),
    ]

    # parquet 18개 고정 컬럼 — S열 이후 동적 컬럼 판별용
    PARQUET_FIXED = {
        "관측소코드", "관측소명", "영문명", "수계명", "소유역코드",
        "관리기관", "폐쇄여부", "관측종류", "개설일", "주소",
        "경도", "위도", "표고(m)", "시자료_시작", "시자료_종료",
        "일자료_시작", "일자료_종료", "군집"
    }
    dynamic_cols = [c for c in sta_df.columns if c not in PARQUET_FIXED]

    # 행2 헤더: 고정14 + 시자료_시작(강우DATA) + 시자료_종료(강우DATA) + 군집 + 동적
    row2_headers = [spec[0] for spec in FIXED_SPEC] + ["시자료_시작", "시자료_종료", "군집"] + dynamic_cols

    # 선택 코드로 sta_df 필터링 (parquet 원본 순서 유지)
    code_set = set(str(c) for c in selected_codes)
    sta_filtered = sta_df[sta_df["관측소코드"].astype(str).isin(code_set)]

    # hourly.parquet 기반 관측소별 전체 기간 사전 계산
    hourly_range = {}
    for code in code_set:
        rows = df_hourly_original[df_hourly_original["STN_CD"].astype(str) == code]
        if rows.empty:
            hourly_range[code] = ("", "")
            continue
        try:
            dates = rows.apply(
                lambda r: f"{int(r.YEAR)}-{int(r.MONTH):02d}-{int(r.DAY):02d}"
                if pd.notna(r.YEAR) and pd.notna(r.MONTH) and pd.notna(r.DAY) else "",
                axis=1
            )
            dates = sorted(d for d in dates if d)
            hourly_range[code] = (dates[0], dates[-1]) if dates else ("", "")
        except Exception:
            hourly_range[code] = ("", "")

    def is_empty(val):
        return pd.isna(val) or str(val).strip() in ("", "nan", "NaN")

    # 데이터 행 구성
    rows_data = []
    for _, stn_row in sta_filtered.iterrows():
        code = str(stn_row["관측소코드"]).strip()
        api_info = None  # lazy 로드

        data_row = []
        for _out_hdr, parq_col, api_field in FIXED_SPEC:
            val = stn_row.get(parq_col, "")
            if is_empty(val) and api_field:
                if api_info is None:
                    try:
                        api_info = fetch_obsinfo(code)
                    except Exception:
                        api_info = {}
                val = api_info.get(api_field, "") if api_info else ""
            val_str = "" if is_empty(val) else str(val).strip()
            if parq_col in ("경도", "위도") and val_str:
                val_str = decimal_to_dms(val_str)
            data_row.append(val_str)

        # O, P: 강우DATA 시작/종료 (hourly.parquet 전체 기간)
        o_start, p_end = hourly_range.get(code, ("", ""))
        data_row.append(o_start)
        data_row.append(p_end)

        # Q: 군집
        cluster_val = stn_row.get("군집", "")
        if is_empty(cluster_val):
            cluster_str = ""
        else:
            try:
                cluster_str = str(int(float(str(cluster_val))))
            except Exception:
                cluster_str = str(cluster_val).strip()
        data_row.append(cluster_str)

        # 동적 컬럼 (R~)
        for dc in dynamic_cols:
            v = stn_row.get(dc, "")
            data_row.append("" if is_empty(v) else str(v).strip())

        rows_data.append(data_row)

    # xlsxwriter로 저장
    out_path = os.path.join(save_dir, "0. 관측소제원.xlsx")
    workbook = xlsxwriter.Workbook(out_path)
    ws = workbook.add_worksheet("관측소제원")

    hdr_fmt = workbook.add_format({
        "bold": True, "bg_color": "#1c9432",
        "font_color": "white", "border": 1,
        "align": "center", "valign": "vcenter"
    })
    hdr_bg_fmt = workbook.add_format({
        "bg_color": "#1c9432", "border": 1
    })
    data_fmt = workbook.add_format({
        "border": 1, "align": "center", "valign": "vcenter"
    })

    n_cols = len(row2_headers)

    # 행1: 전 열 배경색, M:N 병합 '제원', O:P 병합 '강우DATA'
    for ci in range(n_cols):
        ws.write(0, ci, "", hdr_bg_fmt)
    ws.merge_range(0, 12, 0, 13, "제원", hdr_fmt)
    ws.merge_range(0, 14, 0, 15, "강우DATA", hdr_fmt)

    # 행2: 컬럼명
    for ci, hdr in enumerate(row2_headers):
        ws.write(1, ci, hdr, hdr_fmt)

    # 데이터 행
    for ri, row_data in enumerate(rows_data):
        for ci, val in enumerate(row_data):
            ws.write(ri + 2, ci, val, data_fmt)

    # 열 너비 자동 조정
    for ci, hdr in enumerate(row2_headers):
        col_vals = [r[ci] for r in rows_data]
        max_len = max(len(hdr), max((len(str(v)) for v in col_vals), default=0))
        ws.set_column(ci, ci, min(max_len + 2, 35))

    workbook.close()


class RainfallApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("강우자료 통합 관리 및 분석 시스템 v2.0")
        self.resize(1200, 800)
        
        # 메인 위젯
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        
        main_layout = QVBoxLayout(main_widget)
        
        # 비밀 모드 관련 변수
        self.admin_click_count = 0
        self.admin_click_timer = QTimer()
        self.admin_click_timer.setInterval(1000) # 1초 안에 5강 클릭
        self.admin_click_timer.timeout.connect(self.reset_admin_clicks)
        
        # 상단 패널 (정보 및 히든 트릭)
        top_layout = QHBoxLayout()
        title_lbl = QLabel(f"강우자료 통합 관리 및 분석 시스템 v{APP_VERSION}")
        title_lbl.setStyleSheet("font-size: 16pt; font-weight: bold; color: #1C9432;")
        
        btn_info = QPushButton("ℹ️ 프로그램 정보")
        btn_info.clicked.connect(self.show_info_panel)
        btn_info.setStyleSheet("background-color: #E8F5E9; color: #1C9432; font-weight: bold; border-radius: 5px; padding: 5px;")
        
        # 히든 관리자 모드용 (더블 클릭 이벤트 캐치를 위해 이벤트 필터 장착)
        title_lbl.installEventFilter(self)
        self.title_lbl = title_lbl
        
        top_layout.addWidget(title_lbl)
        top_layout.addStretch()
        top_layout.addWidget(btn_info)
        
        main_layout.addLayout(top_layout)
        
        content_layout = QHBoxLayout()
        
        # 좌측 패널 (관측소 목록)를 우측과 2 : 1 비율로 더 넓게 설정 (스크롤 확보)
        left_panel = self.create_left_panel()
        content_layout.addWidget(left_panel, 2)  
        
        # 우측 패널 (기능 탭 및 로그)
        right_panel = self.create_right_panel()
        content_layout.addWidget(right_panel, 1) 
        
        main_layout.addLayout(content_layout)
        
        # 관측소 데이터 로드
        self._load_station_table(os.path.join(BASE_DIR, "data", "Sta1_db.parquet"))
        self.refresh_db_combos()

    def create_left_panel(self):
        panel = QGroupBox("관측소 Master List")
        layout = QVBoxLayout(panel)

        self.station_panel_label = QLabel("관측소 DB: Sta1_db (내장 DB 기반)")
        self.station_panel_label.setStyleSheet("font-size: 8pt; color: gray;")
        layout.addWidget(self.station_panel_label)

        # 검색창
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("검색:"))
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("관측소명, 코드 등 입력...")
        self.search_input.textChanged.connect(self.filter_by_search)
        search_layout.addWidget(self.search_input)
        
        # 군집 멀티 체크박스 패널 (사진처럼 좌측 체크박스, 우측 텍스트로 밀집된 그리드 배치)
        cluster_group = QGroupBox("군집 다중 선택 (1~26)")
        from PyQt6.QtWidgets import QGridLayout
        cluster_layout = QGridLayout(cluster_group)
        cluster_layout.setSpacing(10) # 촘촘한 간격
        self.cluster_checkboxes = {}
        
        # 1~26 군집을 다중 열(예: 한 줄에 7개씩)로 배치
        cols_per_row = 7
        for i in range(26):
            cluster_num = i + 1
            # 텍스트가 체크박스 오른쪽에 오도록 설정 (기본 동작이므로 그대로 사용)
            chk = QCheckBox(str(cluster_num))
            chk.stateChanged.connect(self.on_cluster_checked)
            self.cluster_checkboxes[str(cluster_num)] = chk
            
            row = i // cols_per_row
            col = i % cols_per_row
            cluster_layout.addWidget(chk, row, col)
        
        # 전체 선택/해제
        btn_layout = QHBoxLayout()
        self.btn_select_all = QPushButton("전체 선택")
        self.btn_deselect_all = QPushButton("전체 해제")
        self.btn_select_all.clicked.connect(lambda: self.set_all_checkboxes(True))
        self.btn_deselect_all.clicked.connect(lambda: self.set_all_checkboxes(False))
        btn_layout.addWidget(self.btn_select_all)
        btn_layout.addWidget(self.btn_deselect_all)
        
        # 테이블 
        self.station_table = QTableWidget(0, 6) # 선택, 군집, 코드, 이름, 대유역명, 기관
        self.station_table.setHorizontalHeaderLabels(["선택", "군집", "관측소코드", "관측소명", "대유역명", "관할기관"])
        
        # 열 너비 수동 최적화 및 유동적 확장
        header = self.station_table.horizontalHeader()
        self.station_table.setColumnWidth(0, 40)  # 선택
        self.station_table.setColumnWidth(1, 40)  # 군집
        self.station_table.setColumnWidth(2, 90) # 관측소코드
        self.station_table.setColumnWidth(4, 90)  # 대유역명
        self.station_table.setColumnWidth(5, 110)  # 관할기관
        
        # 관측소명칭은 남은 공간 내용에 맞춰 온전히 표시
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        
        self.station_table.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        
        self.station_table.verticalHeader().setVisible(False)
        
        layout.addLayout(search_layout)
        layout.addWidget(cluster_group)
        layout.addLayout(btn_layout)
        layout.addWidget(self.station_table)
        
        return panel

    def create_right_panel(self):
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # 탭 위젯 생성
        self.tabs = QTabWidget()
        
        # 1. 내장 고속 다운로드 탭
        self.tab_download_db = QWidget()
        self.setup_download_db_tab(self.tab_download_db)
        self.tabs.addTab(self.tab_download_db, "내장 DB 기반 강우 추출")
        
        # [NEW] DB 버전 관리 및 병합 탭 (숨김 상태 보관용)
        self.tab_db_management = QWidget()
        self.setup_db_management_tab(self.tab_db_management)
        
        # 2. 외부 API 실시간 다운로드 탭
        self.tab_download_api = QWidget()
        self.setup_download_api_tab(self.tab_download_api)
        self.tabs.addTab(self.tab_download_api, "외부 API 실시간 다운로드")

        self.tabs.currentChanged.connect(self._on_tab_changed)
        
        # 관리자용 업데이트 탭 (숨김 상태 보관용)
        self.tab_update = QWidget()
        self.setup_update_tab(self.tab_update)
        
        # 관리자용 DB 편집기 탭 (숨김 상태 보관용)
        self.tab_db_editor = QWidget()
        self.setup_db_editor_tab(self.tab_db_editor)

        # 관리자용 관측소 DB 생성 탭 (숨김 상태 보관용)
        self.tab_sta_db = QWidget()
        self.setup_sta_db_tab(self.tab_sta_db)

        # 관리자용 설정 탭 (숨김 상태 보관용)
        self.tab_settings = QWidget()
        self.setup_settings_tab(self.tab_settings)

        self.is_admin_mode = False
        
        layout.addWidget(self.tabs, 3)
        
        # 로그 패널 및 상태바
        log_group = QGroupBox("실행 로그 및 진행 상태")
        log_layout = QVBoxLayout(log_group)
        self.log_console = QTextEdit()
        self.log_console.setReadOnly(True)
        self.log_console.setStyleSheet("font-family: Consolas, 맑은 고딕; font-size: 9pt;")
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        
        log_layout.addWidget(self.log_console)
        log_layout.addWidget(self.progress_bar)
        
        layout.addWidget(log_group, 1)
        
        return panel

    def get_db_versions(self):
        if not os.path.exists(DB_DIR):
            os.makedirs(DB_DIR, exist_ok=True)
        return [d for d in os.listdir(DB_DIR) if os.path.isdir(os.path.join(DB_DIR, d))]

    def refresh_db_combos(self):
        versions = self.get_db_versions()

        if hasattr(self, 'settings_db_combo'):
            cfg = self._load_config()
            saved = cfg.get("target_db_version", "")
            self.settings_db_combo.blockSignals(True)
            self.settings_db_combo.clear()
            self.settings_db_combo.addItems(versions)
            if saved in versions:
                self.settings_db_combo.setCurrentText(saved)
            self.settings_db_combo.blockSignals(False)
            self._on_settings_db_changed(self.settings_db_combo.currentText())

        self.merge_combo1.clear()
        self.merge_combo1.addItems(versions)
        self.merge_combo2.clear()
        self.merge_combo2.addItems(versions)

        if hasattr(self, 'report_combo'):
            self.report_combo.clear()
            self.report_combo.addItems(versions)

        if hasattr(self, 'editor_db_combo'):
            self.editor_db_combo.clear()
            self.editor_db_combo.addItems(versions)

    def setup_settings_tab(self, tab):
        layout = QVBoxLayout(tab)
        group = QGroupBox("내장 DB 기반 엑셀 추출 설정")
        form = QHBoxLayout(group)
        form.addWidget(QLabel("추출 타겟 DB 버전:"))
        self.settings_db_combo = QComboBox()
        form.addWidget(self.settings_db_combo)
        form.addStretch()
        layout.addWidget(group)
        layout.addStretch()
        self.settings_db_combo.currentTextChanged.connect(self._on_settings_db_changed)

    def _load_config(self):
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save_config(self, key, value):
        cfg = self._load_config()
        cfg[key] = value
        try:
            with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                json.dump(cfg, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def _on_settings_db_changed(self, version_name):
        self._save_config("target_db_version", version_name)
        if hasattr(self, 'db_target_label'):
            self.db_target_label.setText(f"추출 타겟 DB: {version_name}  (변경은 관리자 설정 탭에서)")

    def setup_update_tab(self, tab):
        layout = QVBoxLayout(tab)
        
        desc = QLabel("※ 주의사항: 2017년 과거원본 데이터베이스 폴더 위에 덧붙이는 것이 아니라 새로 수집된 데이터만 묶어 '신규 DB 버전' 폴더를 생성합니다.")
        desc.setStyleSheet("color: #FFB74D; font-weight: bold;")
        
        group = QGroupBox("새로운 DB 생성 설정")
        form_layout = QHBoxLayout(group)
        form_layout.addWidget(QLabel("생성할 신규 DB 세트명:"))
        self.new_db_name_input = QLineEdit()
        self.new_db_name_input.setText("v2018_2023_api")
        form_layout.addWidget(self.new_db_name_input)
        form_layout.addSpacing(20)
        
        form_layout.addWidget(QLabel("시작 연도:"))
        self.update_start_year = QSpinBox()
        self.update_start_year.setRange(1950, 2099)
        self.update_start_year.setValue(2018)
        
        form_layout.addWidget(QLabel(" ~ 종료 연도:"))
        self.update_end_year = QSpinBox()
        self.update_end_year.setRange(1950, 2099)
        self.update_end_year.setValue(2023)
        
        form_layout.addWidget(self.update_start_year)
        form_layout.addWidget(self.update_end_year)
        form_layout.addStretch()
        
        self.btn_admin_update = QPushButton("선택한 관측소 1. 시강우 DB 세트 신규 생성")
        self.btn_admin_update.setMinimumHeight(50)
        self.btn_admin_update.setStyleSheet("font-weight: bold; font-size: 11pt;")
        self.btn_admin_update.clicked.connect(self.run_admin_db_update)
        
        self.btn_admin_kma_only_update = QPushButton(f"선택한 관측소 2. {ApiDownloadThread.TARGET_ORG_KMA} 일/연최대 DB만 신규 생성")
        self.btn_admin_kma_only_update.setMinimumHeight(40)
        self.btn_admin_kma_only_update.setStyleSheet("background-color: #e3f2fd; font-weight: bold; font-size: 11pt;")
        self.btn_admin_kma_only_update.clicked.connect(self.run_admin_kma_only_update)
        
        layout.addWidget(desc)
        layout.addWidget(group)
        layout.addStretch()
        layout.addWidget(self.btn_admin_update)
        layout.addWidget(self.btn_admin_kma_only_update)

    def setup_db_management_tab(self, tab):
        layout = QVBoxLayout(tab)
        
        desc = QLabel("내장된 Parquet DB 버전 세트(시강우, 고정, 임의)를 관리하고 두 개의 DB를 병합합니다.")
        
        group = QGroupBox("DB 병합(Merge) 실행")
        form_layout = QHBoxLayout(group)
        
        form_layout.addWidget(QLabel("베이스 DB:"))
        self.merge_combo1 = QComboBox()
        
        form_layout.addWidget(QLabel(" + 덮어쓸 DB(신규):"))
        self.merge_combo2 = QComboBox()
        
        form_layout.addWidget(QLabel(" = 새 병합 DB명:"))
        self.merge_target_name = QLineEdit()
        self.merge_target_name.setText("v2017_2023_merged")
        
        form_layout.addWidget(self.merge_combo1)
        form_layout.addWidget(self.merge_combo2)
        form_layout.addWidget(self.merge_target_name)
        form_layout.addStretch()
        
        btn_merge = QPushButton("선택한 DB 병합 실행")
        btn_merge.setMinimumHeight(50)
        btn_merge.setStyleSheet("font-weight: bold; font-size: 11pt;")
        btn_merge.clicked.connect(self.run_db_merge)
        
        layout.addWidget(desc)
        layout.addWidget(group)
        
        # [NEW] DB 리포트 생성 그룹
        report_group = QGroupBox("DB 검토 요약 리포트 (Review Report)")
        rep_layout = QHBoxLayout(report_group)
        
        rep_layout.addWidget(QLabel("리포트를 추출할 DB 버전:"))
        self.report_combo = QComboBox()
        rep_layout.addWidget(self.report_combo)
        rep_layout.addStretch()
        
        btn_report = QPushButton("선택한 DB 요약 엑셀 리포트 추출")
        btn_report.setMinimumHeight(50)
        btn_report.setStyleSheet("font-weight: bold; font-size: 11pt;")
        btn_report.clicked.connect(self.run_db_report)
        
        layout.addWidget(report_group)
        layout.addStretch()
        
        btn_layout = QHBoxLayout()
        btn_layout.addWidget(btn_merge)
        btn_layout.addWidget(btn_report)
        
        layout.addLayout(btn_layout)

    def setup_sta_db_tab(self, tab):
        layout = QVBoxLayout(tab)

        desc = QLabel("WAMIS 관측소 제원을 수집하고 Sta1/Sta2 DB를 Parquet으로 변환합니다.")
        layout.addWidget(desc)

        # 1. 최신 관측소 제원 불러오기
        grp1 = QGroupBox("1. 최신 관측소 제원 불러오기 (fetch_station_db.py)")
        g1_layout = QVBoxLayout(grp1)
        g1_desc = QLabel(
            "WAMIS rf_dubrfobs → rf_obsinfo API로 전체 관측소 제원을 수집합니다.\n"

            "완료 후 wamis_station_db.xlsx 에 '군집', '관측소코드-이전', '관측소명-이전' 헤더 컬럼이 추가됩니다."
        )
        g1_desc.setWordWrap(True)
        self.btn_fetch_sta = QPushButton("관측소 제원 수집 시작")
        self.btn_fetch_sta.setMinimumHeight(50)
        self.btn_fetch_sta.setStyleSheet("font-weight: bold; font-size: 11pt;")
        self.btn_fetch_sta.clicked.connect(self.run_fetch_station_db)
        g1_layout.addWidget(g1_desc)
        g1_layout.addWidget(self.btn_fetch_sta)
        layout.addWidget(grp1)

        # 2. DB 생성 (Parquet 변환)
        grp2 = QGroupBox("2. 관측소 DB 생성 (convert_sta_to_parquet.py)")
        g2_layout = QVBoxLayout(grp2)
        g2_desc = QLabel(
            "Sta1_db.xlsx / Sta2_db.xlsx 를 읽어 Sta1_db.parquet / Sta2_db.parquet 를 생성합니다.\n"
            "생성된 파일은 data/ 폴더로 자동 이동됩니다."
        )
        g2_desc.setWordWrap(True)
        self.btn_convert_sta = QPushButton("Parquet DB 생성")
        self.btn_convert_sta.setMinimumHeight(50)
        self.btn_convert_sta.setStyleSheet("font-weight: bold; font-size: 11pt;")
        self.btn_convert_sta.clicked.connect(self.run_convert_sta_to_parquet)
        g2_layout.addWidget(g2_desc)
        g2_layout.addWidget(self.btn_convert_sta)
        layout.addWidget(grp2)

        layout.addStretch()

    def setup_db_editor_tab(self, tab):
        layout = QVBoxLayout(tab)
        
        desc = QLabel("내장된 6가지 파케이 DB 파일을 엑셀처럼 표 형태로 보고 마음대로 수정합니다.")
        
        group = QGroupBox("DB 로드 및 저장")
        form_layout = QHBoxLayout(group)
        
        form_layout.addWidget(QLabel("DB 버전:"))
        self.editor_db_combo = QComboBox()
        
        form_layout.addWidget(QLabel("파일:"))
        self.editor_file_combo = QComboBox()
        self.editor_file_combo.addItems([
            "hourly.parquet", 
            "fixed_max.parquet", 
            "arb_max.parquet", 
            "kma_daily_max.parquet", 
            "kma_yearly_max.parquet",
            "arb_max_modified.parquet"
        ])
        
        btn_load = QPushButton("불러오기")
        btn_load.clicked.connect(self.load_db_to_editor)
        
        btn_save = QPushButton("DB에 덮어쓰기")
        btn_save.clicked.connect(self.save_db_from_editor)
        btn_save.setStyleSheet("background-color: #ffebee; font-weight: bold;")
        
        form_layout.addWidget(self.editor_db_combo)
        form_layout.addWidget(self.editor_file_combo)
        form_layout.addWidget(btn_load)
        form_layout.addStretch()
        form_layout.addWidget(btn_save)
        
        layout.addWidget(desc)
        layout.addWidget(group)
        
        self.editor_table = QTableView()
        layout.addWidget(self.editor_table)
        
        self.editor_current_path = None

    def load_db_to_editor(self):
        db_version = self.editor_db_combo.currentText()
        file_name = self.editor_file_combo.currentText()
        
        if not db_version or not file_name:
            return
            
        path = os.path.join(DB_DIR, db_version, file_name)
        if not os.path.exists(path):
            QMessageBox.warning(self, "오류", f"해당 파일이 존재하지 않습니다.\n{path}")
            return
            
        try:
            df = pd.read_parquet(path)
            self.editor_current_path = path
            
            self.editor_model = PandasModel(df)
            self.editor_table.setModel(self.editor_model)
            
            self.log_msg(f"✅ DB 에디터: {file_name} 불러오기 완료 ({len(df)}행)")
        except Exception as e:
            QMessageBox.critical(self, "오류", f"파일을 불러오는 중 오류 발생:\n{e}")

    def save_db_from_editor(self):
        if not hasattr(self, 'editor_model') or self.editor_model._data is None:
            QMessageBox.warning(self, "오류", "저장할 데이터가 없습니다.")
            return
            
        try:
            self.editor_model._data.to_parquet(self.editor_current_path, index=False)
            self.log_msg(f"✅ DB 에디터: 수정된 내용을 덮어썼습니다. ({self.editor_current_path})")
            QMessageBox.information(self, "저장 완료", "DB 파일이 성공적으로 덮어써졌습니다.")
        except Exception as e:
            QMessageBox.critical(self, "오류", f"저장 중 오류 발생:\n{e}")

    def setup_download_db_tab(self, tab):
        layout = QVBoxLayout(tab)

        group = QGroupBox("추출 설정")
        form_layout = QHBoxLayout(group)

        self.db_target_label = QLabel("추출 타겟 DB: (미설정)  (변경은 관리자 설정 탭에서)")
        self.db_target_label.setStyleSheet("color: #555; font-style: italic;")
        form_layout.addWidget(self.db_target_label)
        form_layout.addSpacing(20)

        form_layout.addWidget(QLabel("시작 연도:"))
        self.db_start_year = QSpinBox()
        self.db_start_year.setRange(1900, 2099)
        self.db_start_year.setValue(1970)

        form_layout.addWidget(QLabel(" ~ 종료 연도:"))
        self.db_end_year = QSpinBox()
        self.db_end_year.setRange(1900, 2099)
        self.db_end_year.setValue(2023)

        form_layout.addWidget(self.db_start_year)
        form_layout.addWidget(self.db_end_year)
        form_layout.addStretch()

        self.btn_db_extract = QPushButton("내장 DB에서 엑셀 파일 생성하기")
        self.btn_db_extract.setMinimumHeight(50)
        self.btn_db_extract.setStyleSheet("font-weight: bold; font-size: 11pt;")
        self.btn_db_extract.clicked.connect(self.run_db_extraction)

        desc_tabs = QTabWidget()
        desc_tab = QWidget()
        desc_tab_layout = QVBoxLayout(desc_tab)
        desc_label = QLabel("~2017년: RFAHD 강우자료\n2018년~: wamis 및 기상청 제공 강우자료")
        desc_label.setWordWrap(True)
        desc_label.setAlignment(Qt.AlignmentFlag.AlignTop)
        desc_tab_layout.addWidget(desc_label)
        desc_tabs.addTab(desc_tab, "설명")

        layout.addWidget(group)
        layout.addWidget(desc_tabs)
        layout.addStretch()
        layout.addWidget(self.btn_db_extract)

    def setup_download_api_tab(self, tab):
        layout = QVBoxLayout(tab)

        group = QGroupBox("실시간 API 대상 설정")
        form_layout = QHBoxLayout(group)
        form_layout.addWidget(QLabel("시작일:"))
        self.api_start_date = QDateEdit()
        self.api_start_date.setDisplayFormat("yyyy-MM-dd")
        self.api_start_date.setDate(QDate(2020, 1, 1))
        self.api_start_date.setCalendarPopup(True)
        form_layout.addWidget(self.api_start_date)
        form_layout.addWidget(QLabel(" ~ 종료일:"))
        self.api_end_date = QDateEdit()
        self.api_end_date.setDisplayFormat("yyyy-MM-dd")
        self.api_end_date.setDate(QDate.currentDate())
        self.api_end_date.setCalendarPopup(True)
        form_layout.addWidget(self.api_end_date)
        form_layout.addStretch()

        self.btn_api_download = QPushButton("외부 API 실시간 통신 및 엑셀 즉시 추출")
        self.btn_api_download.setMinimumHeight(50)
        self.btn_api_download.setStyleSheet("font-weight: bold; font-size: 11pt;")
        self.btn_api_download.clicked.connect(self.run_api_download)

        api_desc_tabs = QTabWidget()
        api_desc_tab = QWidget()
        api_desc_tab_layout = QVBoxLayout(api_desc_tab)
        api_desc_label = QLabel("wamis 및 기상청 제공 강우자료 다운로드")
        api_desc_label.setWordWrap(True)
        api_desc_label.setAlignment(Qt.AlignmentFlag.AlignTop)
        api_desc_tab_layout.addWidget(api_desc_label)
        api_desc_tabs.addTab(api_desc_tab, "설명")

        layout.addWidget(group)
        layout.addWidget(api_desc_tabs)
        layout.addStretch()
        layout.addWidget(self.btn_api_download)

    def _load_station_table(self, parquet_path):
        db_name = os.path.basename(parquet_path)

        # 클러스터 체크박스 및 검색창 초기화
        self.search_input.blockSignals(True)
        self.search_input.clear()
        self.search_input.blockSignals(False)
        for chk in self.cluster_checkboxes.values():
            chk.blockSignals(True)
            chk.setChecked(False)
            chk.blockSignals(False)

        if not os.path.exists(parquet_path):
            self.station_table.setRowCount(0)
            self.station_panel_label.setText(f"⚠️ {db_name} 파일 없음")
            self.log_msg(f"에러: {parquet_path} 파일을 찾을 수 없습니다.")
            return

        try:
            df = pd.read_parquet(parquet_path)
            valid_stations = []
            for _, row in df.iterrows():
                code_val = str(row["관측소코드"]).strip()

                if len(code_val) >= 7 and code_val.isdigit():
                    cluster_raw = row["군집"]
                    cluster_val = "-" if pd.isna(cluster_raw) else str(int(cluster_raw))

                    name_val = str(row["관측소명"]).strip()
                    if name_val == "nan":
                        name_val = ""

                    valid_stations.append({
                        'cluster': cluster_val,
                        'code': code_val,
                        'name': name_val,
                        'basin': str(row["수계명"]).strip() if pd.notna(row["수계명"]) else "",
                        'org': str(row["관리기관"]).strip() if pd.notna(row["관리기관"]) else ""
                    })

            self.station_table.setRowCount(len(valid_stations))
            for i, stn in enumerate(valid_stations):
                chk_box = QCheckBox()
                chk_box.setChecked(False)
                chk_widget = QWidget()
                chk_layout = QHBoxLayout(chk_widget)
                chk_layout.addWidget(chk_box)
                chk_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
                chk_layout.setContentsMargins(0, 0, 0, 0)
                self.station_table.setCellWidget(i, 0, chk_widget)

                cols = [stn['cluster'], stn['code'], stn['name'], stn['basin'], stn['org']]
                for j, text in enumerate(cols):
                    item = QTableWidgetItem(text)
                    item.setFlags(item.flags() ^ Qt.ItemFlag.ItemIsEditable)
                    item.setTextAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
                    self.station_table.setItem(i, j + 1, item)

            # 모든 행 표시 (이전 검색 필터 해제)
            for i in range(self.station_table.rowCount()):
                self.station_table.setRowHidden(i, False)

            self.station_panel_label.setText(f"✅ {db_name} ({len(valid_stations)}개 관측소)")
            self.log_msg(f"관측소 총 {len(valid_stations)}개 인식 완료.")
        except Exception as e:
            self.station_panel_label.setText(f"❌ {db_name} 로딩 실패")
            self.log_msg(f"관측소 로딩 실패: {e}")

    def _on_tab_changed(self, index):
        widget = self.tabs.widget(index)
        if widget is self.tab_download_db:
            self._load_station_table(os.path.join(BASE_DIR, "data", "Sta1_db.parquet"))
        elif widget is self.tab_download_api:
            self._load_station_table(os.path.join(BASE_DIR, "data", "Sta2_db.parquet"))
        elif widget is self.tab_update:
            self._load_station_table(os.path.join(BASE_DIR, "data", "Sta1_db.parquet"))

    def run_api_download(self):
        if self.station_table.rowCount() == 0:
            QMessageBox.warning(self, "경고", "관측소 DB가 로딩되지 않았습니다.\ndata/Sta2_db.parquet 파일을 먼저 생성하세요.")
            return

        selected_stations = []
        for i in range(self.station_table.rowCount()):
            widget = self.station_table.cellWidget(i, 0)
            if widget:
                chk = widget.findChild(QCheckBox)
                if chk and chk.isChecked():
                    cluster = self.station_table.item(i, 1).text()
                    code = self.station_table.item(i, 2).text()
                    name = self.station_table.item(i, 3).text()
                    org = self.station_table.item(i, 5).text()
                    selected_stations.append({'cluster': cluster, 'code': code, 'name': name, 'org': org})

        if not selected_stations:
            QMessageBox.warning(self, "경고", "다운로드할 관측소를 먼저 선택해주세요!")
            return

        start_qdate = self.api_start_date.date()
        end_qdate = self.api_end_date.date()
        if start_qdate > end_qdate:
            QMessageBox.warning(self, "오류", "시작일이 종료일보다 늦을 수 없습니다.")
            return

        start_date_str = start_qdate.toString("yyyyMMdd")
        end_date_str = end_qdate.toString("yyyyMMdd")
        start_yr = start_qdate.year()
        end_yr = end_qdate.year()

        save_dir = QFileDialog.getExistingDirectory(self, "결과물을 저장할 폴더를 선택하세요")
        if not save_dir:
            return

        save_dir = _make_output_subdir(save_dir)

        temp_path = os.path.join(TEMP_DIR, "api_download_temp")
        os.makedirs(temp_path, exist_ok=True)

        self._api_temp_db = temp_path
        self._api_save_dir = save_dir
        self._api_start_yr = start_yr
        self._api_end_yr = end_yr
        self._api_selected_stations = selected_stations

        self._set_ui_enabled(False)
        self.progress_bar.setValue(0)
        self.log_msg(f"[API 다운로드] {start_date_str}~{end_date_str} / {len(selected_stations)}개 관측소")

        self.api_download_thread = ApiDownloadThread(
            selected_stations, start_yr, end_yr, "temp",
            save_path=temp_path, start_date=start_date_str, end_date=end_date_str
        )
        self.api_download_thread.log_signal.connect(self.log_msg)
        self.api_download_thread.progress_signal.connect(
            lambda v: self.progress_bar.setValue(int(v * 0.9))
        )
        self.api_download_thread.finished_signal.connect(self.on_api_download_finished)
        self.api_download_thread.start()

    def on_api_download_finished(self, success, msg):
        if not success:
            self._set_ui_enabled(True)
            self.log_msg(f"❌ {msg}")
            QMessageBox.critical(self, "실패", msg)
            self._cleanup_temp_db()
            return
        sta_parquet_path = os.path.join(BASE_DIR, "data", "Sta2_db.parquet")
        self.extraction_thread = ExtractionThread(
            self._api_temp_db, self._api_save_dir,
            self._api_start_yr, self._api_end_yr,
            self._api_selected_stations, sta_parquet_path
        )
        self.extraction_thread.log_signal.connect(self.log_msg)
        self.extraction_thread.progress_signal.connect(
            lambda v: self.progress_bar.setValue(int(90 + v * 0.1))
        )
        self.extraction_thread.finished_signal.connect(self.on_extraction_finished)
        self.extraction_thread.start()

    def on_extraction_finished(self, success, result_msg):
        self._set_ui_enabled(True)
        if hasattr(self, '_api_temp_db'):
            self._cleanup_temp_db()
        if success:
            self.progress_bar.setValue(100)
            QMessageBox.information(self, "완료", "데이터 추출이 완료되었습니다.")
        else:
            self.log_msg(f"❌ 추출 실패: {result_msg}")
            QMessageBox.critical(self, "실패", result_msg)

    def _cleanup_temp_db(self):
        if hasattr(self, '_api_temp_db') and os.path.exists(self._api_temp_db):
            shutil.rmtree(self._api_temp_db, ignore_errors=True)
            self.log_msg("임시 DB 정리 완료")

    def _set_ui_enabled(self, enabled: bool):
        """추출/다운로드 진행 중 UI 전체 잠금 (프로그램 정보 버튼 제외)."""
        self.tabs.tabBar().setEnabled(enabled)
        # 항상 존재하는 버튼 및 위젯
        for w in [
            self.btn_db_extract, self.btn_api_download,
            self.btn_select_all, self.btn_deselect_all,
            self.search_input, self.station_table,
            self.db_start_year, self.db_end_year,
            self.api_start_date, self.api_end_date,
            self.settings_db_combo,
        ]:
            w.setEnabled(enabled)
        for chk in self.cluster_checkboxes.values():
            chk.setEnabled(enabled)
        # 관리자 모드에서만 존재하는 위젯 (hasattr 방어)
        for attr in [
            'btn_admin_update', 'btn_admin_kma_only_update',
            'btn_fetch_sta', 'btn_convert_sta',
            'new_db_name_input', 'update_start_year', 'update_end_year',
            'merge_target_name', 'merge_combo1', 'merge_combo2',
            'report_combo', 'editor_db_combo', 'editor_file_combo',
        ]:
            if hasattr(self, attr):
                getattr(self, attr).setEnabled(enabled)

    def filter_by_search(self):
        text = self.search_input.text().lower()
        for i in range(self.station_table.rowCount()):
            match_text = False
            for j in range(1, 6):
                item = self.station_table.item(i, j)
                if item and text in item.text().lower():
                    match_text = True
                    break
            self.station_table.setRowHidden(i, not match_text)

    def on_cluster_checked(self, state):
        # 체크된 군집 목록 수집
        active_clusters = [c_num for c_num, chk in self.cluster_checkboxes.items() if chk.isChecked()]
        
        for i in range(self.station_table.rowCount()):
            cluster_item = self.station_table.item(i, 1) # 1열이 군집
            if cluster_item:
                c_val = cluster_item.text()
                widget = self.station_table.cellWidget(i, 0)
                if widget:
                    chk = widget.findChild(QCheckBox)
                    if chk:
                        # 해당 행의 군집이 활성화된 군집 목록에 있으면 체크, 없으면 해제
                        if c_val in active_clusters:
                            chk.setChecked(True)
                        else:
                            chk.setChecked(False)

    def set_all_checkboxes(self, state):
        for i in range(self.station_table.rowCount()):
            if not self.station_table.isRowHidden(i):
                widget = self.station_table.cellWidget(i, 0)
                if widget:
                    chk = widget.findChild(QCheckBox)
                    if chk:
                        chk.setChecked(state)
        
        # 전체 해제 시 군집 체크박스도 모두 해제 연동
        if not state:
            for chk in self.cluster_checkboxes.values():
                chk.blockSignals(True)
                chk.setChecked(False)
                chk.blockSignals(False)

    def log_msg(self, msg):
        self.log_console.append(msg)

    def reset_admin_clicks(self):
        self.admin_click_count = 0
        self.admin_click_timer.stop()

    def run_admin_db_update(self):
        selected_stations = []
        for i in range(self.station_table.rowCount()):
            widget = self.station_table.cellWidget(i, 0)
            if widget:
                chk = widget.findChild(QCheckBox)
                if chk and chk.isChecked():
                    cluster = self.station_table.item(i, 1).text()
                    code = self.station_table.item(i, 2).text()
                    name = self.station_table.item(i, 3).text()
                    org = self.station_table.item(i, 5).text()
                    selected_stations.append({'cluster': cluster, 'code': code, 'name': name, 'org': org})
        
        if not selected_stations:
            QMessageBox.warning(self, "경고", "업데이트할 관측소를 먼저 선택해주세요!")
            return
            
        start_yr = self.update_start_year.value()
        end_yr = self.update_end_year.value()
        
        if start_yr > end_yr:
            QMessageBox.warning(self, "오류", "종료 연도가 시작 연도보다 빠를 수 없습니다.")
            return
            
        new_db_name = self.new_db_name_input.text().strip()
        if not new_db_name:
            QMessageBox.warning(self, "오류", "신규 DB 버전명을 입력해주세요.")
            return

        self.btn_admin_update.setEnabled(False)
        if hasattr(self, 'btn_admin_kma_only_update'):
            self.btn_admin_kma_only_update.setEnabled(False)
        self.progress_bar.setValue(0)
        
        self.update_thread = ApiDownloadThread(selected_stations, start_yr, end_yr, new_db_name)
        self.update_thread.log_signal.connect(self.log_msg)
        self.update_thread.progress_signal.connect(self.progress_bar.setValue)
        self.update_thread.finished_signal.connect(self.on_admin_db_update_finished)
        self.update_thread.start()
        
    def run_admin_kma_only_update(self):
        selected_stations = []
        for i in range(self.station_table.rowCount()):
            widget = self.station_table.cellWidget(i, 0)
            if widget:
                chk = widget.findChild(QCheckBox)
                if chk and chk.isChecked():
                    cluster = self.station_table.item(i, 1).text()
                    code = self.station_table.item(i, 2).text()
                    name = self.station_table.item(i, 3).text()
                    org = self.station_table.item(i, 5).text()
                    selected_stations.append({'cluster': cluster, 'code': code, 'name': name, 'org': org})
        
        if not selected_stations:
            QMessageBox.warning(self, "경고", "업데이트할 관측소를 먼저 선택해주세요!")
            return
            
        start_yr = self.update_start_year.value()
        end_yr = self.update_end_year.value()
        
        if start_yr > end_yr:
            QMessageBox.warning(self, "오류", "종료 연도가 시작 연도보다 빠를 수 없습니다.")
            return
            
        new_db_name = self.new_db_name_input.text().strip()
        if not new_db_name:
            QMessageBox.warning(self, "오류", "신규 DB 버전명을 입력해주세요.")
            return

        self.btn_admin_update.setEnabled(False)
        if hasattr(self, 'btn_admin_kma_only_update'):
            self.btn_admin_kma_only_update.setEnabled(False)
        self.progress_bar.setValue(0)
        
        self.update_thread = ApiDownloadThread(selected_stations, start_yr, end_yr, new_db_name, kma_only=True)
        self.update_thread.log_signal.connect(self.log_msg)
        self.update_thread.progress_signal.connect(self.progress_bar.setValue)
        self.update_thread.finished_signal.connect(self.on_admin_db_update_finished)
        self.update_thread.start()

    def on_admin_db_update_finished(self, success, msg):
        self.btn_admin_update.setEnabled(True)
        if hasattr(self, 'btn_admin_kma_only_update'):
            self.btn_admin_kma_only_update.setEnabled(True)
        self.refresh_db_combos() # DB 생성 후 UI 갱신
        if success:
            self.log_msg(f"✅ {msg}")
            QMessageBox.information(self, "업데이트 완료", msg)
        else:
            self.log_msg(f"❌ {msg}")
            QMessageBox.critical(self, "업데이트 실패", msg)

    def run_fetch_station_db(self):
        self.btn_fetch_sta.setEnabled(False)
        self.log_msg("\n[관측소 제원 수집] 시작 (수 분 소요될 수 있습니다)...")
        self.fetch_sta_thread = FetchStationThread(BASE_DIR)
        self.fetch_sta_thread.log_signal.connect(self.log_msg)
        self.fetch_sta_thread.finished_signal.connect(self.on_fetch_station_finished)
        self.fetch_sta_thread.start()

    def on_fetch_station_finished(self, success):
        self.btn_fetch_sta.setEnabled(True)
        if success:
            self.log_msg("✅ 관측소 제원 수집 완료. wamis_station_db.xlsx 저장됨.")
            QMessageBox.information(self, "완료", "관측소 제원 수집이 완료되었습니다.\nwamis_station_db.xlsx 를 확인하세요.")
        else:
            self.log_msg("❌ 관측소 제원 수집 실패.")
            QMessageBox.critical(self, "실패", "관측소 제원 수집 중 오류가 발생했습니다. 로그를 확인하세요.")

    def run_convert_sta_to_parquet(self):
        self.btn_convert_sta.setEnabled(False)
        self.log_msg("\n[Parquet DB 생성] 시작...")
        self.convert_sta_thread = ConvertStaThread(BASE_DIR)
        self.convert_sta_thread.log_signal.connect(self.log_msg)
        self.convert_sta_thread.finished_signal.connect(self.on_convert_sta_finished)
        self.convert_sta_thread.start()

    def on_convert_sta_finished(self, success):
        self.btn_convert_sta.setEnabled(True)
        if getattr(self.convert_sta_thread, 'cancelled', False):
            return
        if success:
            self.log_msg("✅ Parquet DB 생성 완료. data/ 폴더에 저장됨.")
            QMessageBox.information(self, "완료", "Parquet DB 생성이 완료되었습니다.\ndata/ 폴더를 확인하세요.")
        else:
            self.log_msg("❌ Parquet DB 생성 실패.")
            QMessageBox.critical(self, "실패", "Parquet DB 생성 중 오류가 발생했습니다. 로그를 확인하세요.")

    def run_db_merge(self):
        db1_name = self.merge_combo1.currentText()
        db2_name = self.merge_combo2.currentText()
        target_name = self.merge_target_name.text().strip()
        
        if not db1_name or not db2_name:
            QMessageBox.warning(self, "오류", "병합할 두 개의 DB를 선택해주세요.")
            return
            
        if not target_name:
            QMessageBox.warning(self, "오류", "새로운 병합 DB 이름을 입력해주세요.")
            return
            
        if db1_name == db2_name:
            QMessageBox.warning(self, "오류", "동일한 DB를 병합할 수 없습니다.")
            return
            
        target_dir = os.path.join(DB_DIR, target_name)
        if os.path.exists(target_dir):
             reply = QMessageBox.question(self, '경고', f'[{target_name}] 버전이 이미 존재합니다. 덮어쓰시겠습니까?', 
                                         QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No)
             if reply == QMessageBox.StandardButton.No:
                 return
                 
        files_to_merge = ["hourly.parquet", "fixed_max.parquet", "arb_max.parquet", 
                          "kma_daily_max.parquet", "kma_yearly_max.parquet", "arb_max_modified.parquet"]
        
        self.log_msg(f"DB 병합 시작: {db1_name} + {db2_name} -> {target_name}")
        
        def merge_dataframes(f1, f2):
            if os.path.exists(f1) and os.path.exists(f2):
                df1 = pd.read_parquet(f1)
                df2 = pd.read_parquet(f2)
                
                # 기준 키는 각 파케이가 보유한 구체적 시간 단위를 추적
                subset = ['STN_CD', 'YEAR']
                columns = df1.columns.tolist()
                
                if 'MONTH' in columns: subset.append('MONTH')
                if 'DAY' in columns: subset.append('DAY')
                if 'HOUR' in columns: subset.append('HOUR')
                if 'MIN' in columns: subset.append('MIN')
                    
                merged = pd.concat([df1, df2], ignore_index=True)
                merged = merged.drop_duplicates(subset=subset, keep='last')
                return merged
            elif os.path.exists(f1):
                return pd.read_parquet(f1)
            elif os.path.exists(f2):
                return pd.read_parquet(f2)
            return pd.DataFrame()

        try:
            os.makedirs(target_dir, exist_ok=True)
            for f in files_to_merge:
                path1 = os.path.join(DB_DIR, db1_name, f)
                path2 = os.path.join(DB_DIR, db2_name, f)
                target_path = os.path.join(target_dir, f)
                
                merged_df = merge_dataframes(path1, path2)
                if not merged_df.empty:
                    merged_df.to_parquet(target_path, index=False)
                    self.log_msg(f" - {f} 병합 완료 (총 {len(merged_df)}행)")
                    
            self.refresh_db_combos()
            self.log_msg("✅ 모든 병합 작업 완료!")
            QMessageBox.information(self, "완료", "DB 병합이 완료되었습니다.")
        except Exception as e:
            self.log_msg(f"❌ 병합 중 오류 발생: {e}")
            QMessageBox.critical(self, "오류", str(e))

    def run_db_report(self):
        db_version = self.report_combo.currentText()
        if not db_version:
            QMessageBox.warning(self, "오류", "리포트를 추출할 DB를 선택해주세요.")
            return
            
        save_path, _ = QFileDialog.getSaveFileName(self, "리포트 저장 위치 선택", f"{db_version}_DB_Review_Report.xlsx", "Excel Files (*.xlsx)")
        if not save_path:
            return
            
        self.log_msg(f"[{db_version}] DB 검토 요약 리포트 생성 중...")
        
        # 시간이 걸릴 수 있으므로 메세지 펌핑
        QApplication.processEvents()
        
        try:
            success, msg = generate_db_review_report(db_version, save_path)
            if success:
                self.log_msg(f"✅ 리포트 생성 완료: {save_path}")
                QMessageBox.information(self, "성공", msg)
            else:
                self.log_msg(f"❌ 리포트 생성 실패: {msg}")
                QMessageBox.critical(self, "실패", msg)
        except Exception as e:
            self.log_msg(f"❌ 리포트 생성 중 예측하지 못한 오류: {e}")
            QMessageBox.critical(self, "오류", str(e))

    def run_db_extraction(self):
        # 1. 체크된 관측소 가져오기
        selected_stations = []
        for i in range(self.station_table.rowCount()):
            widget = self.station_table.cellWidget(i, 0)
            if widget:
                chk = widget.findChild(QCheckBox)
                if chk and chk.isChecked():
                    cluster = self.station_table.item(i, 1).text()
                    code = self.station_table.item(i, 2).text()
                    selected_stations.append({'cluster': cluster, 'code': code})

        if not selected_stations:
            QMessageBox.warning(self, "경고", "추출할 관측소를 먼저 선택해주세요!")
            return

        db_version = self.settings_db_combo.currentText()
        if not db_version:
            QMessageBox.warning(self, "오류", "추출할 DB 버전이 선택되지 않았습니다.")
            return

        start_yr = self.db_start_year.value()
        end_yr = self.db_end_year.value()

        if start_yr > end_yr:
            QMessageBox.warning(self, "경고", "시작 연도가 종료 연도보다 클 수 없습니다.")
            return

        # 2. 저장 위치 선택
        save_dir = QFileDialog.getExistingDirectory(self, "결과물을 저장할 폴더를 선택하세요")
        if not save_dir:
            return

        save_dir = _make_output_subdir(save_dir)

        db_path = os.path.join(DB_DIR, db_version)
        sta_parquet_path = os.path.join(BASE_DIR, "data", "Sta1_db.parquet")
        self._set_ui_enabled(False)
        self.progress_bar.setValue(0)
        self.extraction_thread = ExtractionThread(db_path, save_dir, start_yr, end_yr, selected_stations, sta_parquet_path)
        self.extraction_thread.log_signal.connect(self.log_msg)
        self.extraction_thread.progress_signal.connect(self.progress_bar.setValue)
        self.extraction_thread.finished_signal.connect(self.on_extraction_finished)
        self.extraction_thread.start()

    def eventFilter(self, obj, event):
        # 관리자 모드 진입 트릭: 프로그램 메인 제목 라벨을 5번 연속 클릭할 경우 발생!
        # MouseButtonRelease로 판단하여 클릭 미스를 줄임
        if obj == getattr(self, 'title_lbl', None) and event.type() == QEvent.Type.MouseButtonRelease:
            if self.admin_click_count == 0:
                self.admin_click_timer.start()
                
            self.admin_click_count += 1
            
            if self.admin_click_count >= 5:
                self.reset_admin_clicks()
                if not self.is_admin_mode:
                    self.is_admin_mode = True
                    self.tabs.addTab(self.tab_db_management, "🔒 [관리자] DB 컴포넌트 관리 및 병합")
                    self.tabs.addTab(self.tab_db_editor, "🔒 [관리자] DB 편집 (DB Editor)")
                    self.tabs.addTab(self.tab_update, "🔒 [관리자] 내장 DB 연도별 업데이트")
                    self.tabs.addTab(self.tab_sta_db, "🔒 [관리자] 관측소 DB 생성")
                    self.tabs.addTab(self.tab_settings, "🔒 [관리자] 설정")
                    self.log_msg("\n⚠️ [관리자 모드 활성화] 숨겨진 관리자 전용 모듈이 열렸습니다.")
                else:
                    self.is_admin_mode = False
                    for i in reversed(range(self.tabs.count())):
                        if self.tabs.widget(i) in [self.tab_update, self.tab_db_management, self.tab_db_editor, self.tab_sta_db, self.tab_settings]:
                            self.tabs.removeTab(i)
                    self.log_msg("🔒 [관리자 모드 해제] 관리자 모듈 숨김처리 완료.")
                return True
        return super().eventFilter(obj, event)

    def show_info_panel(self):
        # 팝업 정보창 띄우기
        msg = QMessageBox()
        msg.setWindowTitle("프로그램 정보")
        msg.setIcon(QMessageBox.Icon.Information)
        msg.setText(f"<b>강우자료 통합 관리 및 분석 시스템</b> v{APP_VERSION}<br><br>"
                    f"<b>제작자:</b> {APP_AUTHOR}<br>"
                    f"<b>이메일:</b> chykim1@gmail.com<br>"
                    f"<b>배포일자:</b> {APP_DATE}<br><br>"
                    f"과거(2017) 데이터베이스와 WAMIS/기상청 API 연동을 통한<br>"
                    f"안정적인 단일 엑셀 다운로더입니다.")
        msg.exec()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    
    # qt-material에서 커스텀 색상을 덮어씌울 수 있는 기능 제공
    import os
    # 임시로 커스텀 테마 xml을 만들어 1c9432 적용
    custom_theme = """
    <resources>
        <color name="primaryColor">#1c9432</color>
        <color name="primaryLightColor">#58c65f</color>
        <color name="secondaryColor">#FFFDF5</color>
        <color name="secondaryLightColor">#ffffff</color>
        <color name="secondaryDarkColor">#E6E2D3</color>
        <color name="primaryTextColor">#000000</color>
        <color name="secondaryTextColor">#222222</color>
    </resources>
    """
    with open("custom_theme.xml", "w") as f:
        f.write(custom_theme)
        
    apply_stylesheet(app, theme='custom_theme.xml')
    # apply_stylesheet가 테마 색상을 오버라이드하게 지정 (light_teal 기반에 덮어씌우기)
    # qt-material은 light_teal.xml과 동일한 디렉토리에 있는 color를 참조하므로 
    # 기본 폴백으로 light_teal을 쓰되 커스텀 xml로 색상을 재배치했습니다.
    
    window = RainfallApp()
    window.show()
    sys.exit(app.exec())
