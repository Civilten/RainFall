import sys
import os
import pandas as pd
from PyQt6.QtWidgets import (QApplication, QWidget, QVBoxLayout, QPushButton, 
                             QLabel, QFileDialog, QMessageBox, QHBoxLayout)
from PyQt6.QtCore import Qt

# Import the existing comparison logic if possible, 
# or just inline it for simplicity in this standalone GUI tool.

def compare_excels_logic(file1_path, file2_path, output_path):
    try:
        xl1 = pd.ExcelFile(file1_path)
        xl2 = pd.ExcelFile(file2_path)
        
        common_sheets = sorted(list(set(xl1.sheet_names) & set(xl2.sheet_names)))
        
        with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
            summary_data = []
            
            for sheet in common_sheets:
                df1 = pd.read_excel(xl1, sheet_name=sheet)
                df2 = pd.read_excel(xl2, sheet_name=sheet)
                
                year_col1 = "Year" if "Year" in df1.columns else df1.columns[0]
                year_col2 = "Year" if "Year" in df2.columns else df2.columns[0]
                
                df1 = df1.rename(columns={year_col1: "Year"})
                df2 = df2.rename(columns={year_col2: "Year"})
                
                df1["Year"] = pd.to_numeric(df1["Year"], errors='coerce').fillna(0).astype(int)
                df2["Year"] = pd.to_numeric(df2["Year"], errors='coerce').fillna(0).astype(int)
                
                merged = pd.merge(df1, df2, on="Year", suffixes=('_f1', '_f2'))
                
                cols1 = [c for c in df1.columns if c != "Year"]
                cols2 = [c for c in df2.columns if c != "Year"]
                cols1_str = [str(c) for c in cols1]
                cols2_str = [str(c) for c in cols2]
                common_cols = sorted(list(set(cols1_str) & set(cols2_str)))
                
                diff_records = []
                total_diffs = 0
                
                for col in common_cols:
                    orig_col1 = next((c for c in cols1 if str(c) == col), None)
                    orig_col2 = next((c for c in cols2 if str(c) == col), None)
                    
                    if orig_col1 is None or orig_col2 is None:
                        continue
                    
                    f1_col = f"{orig_col1}_f1"
                    f2_col = f"{orig_col2}_f2"
                    
                    diff = (merged[f1_col] - merged[f2_col]).abs()
                    has_diff = diff > 1e-6
                    
                    if has_diff.any():
                        diff_df = merged[has_diff][["Year", f1_col, f2_col]].copy()
                        diff_df["Column"] = col
                        diff_df["Diff"] = merged[has_diff][f1_col] - merged[has_diff][f2_col]
                        diff_df = diff_df.rename(columns={f1_col: "File1_Val", f2_col: "File2_Val"})
                        diff_records.append(diff_df)
                        total_diffs += has_diff.sum()
                
                if diff_records:
                    final_diff_df = pd.concat(diff_records)
                    final_diff_df = final_diff_df[["Year", "Column", "File1_Val", "File2_Val", "Diff"]]
                    final_diff_df.to_excel(writer, sheet_name=sheet, index=False)
                    summary_data.append({"Sheet": sheet, "Status": "Differences Found", "Diff Count": total_diffs})
                else:
                    summary_data.append({"Sheet": sheet, "Status": "Identical", "Diff Count": 0})
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            
        return True, f"비교 완료!\n결과 파일: {output_path}"
    except Exception as e:
        return False, f"오류 발생: {str(e)}"

class ExcelComparatorApp(QWidget):
    def __init__(self):
        super().__init__()
        self.initUI()
        
    def initUI(self):
        self.setWindowTitle('엑셀 데이터 비교 툴')
        self.setGeometry(300, 300, 500, 250)
        
        layout = QVBoxLayout()
        
        # File 1
        h1 = QHBoxLayout()
        self.label1 = QLabel('파일 1: 선택되지 않음')
        btn1 = QPushButton('파일 1 선택')
        btn1.clicked.connect(self.selectFile1)
        h1.addWidget(self.label1, 7)
        h1.addWidget(btn1, 3)
        layout.addLayout(h1)
        
        # File 2
        h2 = QHBoxLayout()
        self.label2 = QLabel('파일 2: 선택되지 않음')
        btn2 = QPushButton('파일 2 선택')
        btn2.clicked.connect(self.selectFile2)
        h2.addWidget(self.label2, 7)
        h2.addWidget(btn2, 3)
        layout.addLayout(h2)
        
        # Compare Button
        self.compareBtn = QPushButton('비교 시작')
        self.compareBtn.setFixedHeight(50)
        self.compareBtn.clicked.connect(self.runComparison)
        layout.addWidget(self.compareBtn)
        
        self.setLayout(layout)
        self.file1 = ""
        self.file2 = ""
        
    def selectFile1(self):
        fname, _ = QFileDialog.getOpenFileName(self, '파일 1 선택', '', 'Excel files (*.xlsx *.xls)')
        if fname:
            self.file1 = fname
            self.label1.setText(f'파일 1: {os.path.basename(fname)}')
            
    def selectFile2(self):
        fname, _ = QFileDialog.getOpenFileName(self, '파일 2 선택', '', 'Excel files (*.xlsx *.xls)')
        if fname:
            self.file2 = fname
            self.label2.setText(f'파일 2: {os.path.basename(fname)}')
            
    def runComparison(self):
        if not self.file1 or not self.file2:
            QMessageBox.warning(self, '경고', '두 파일을 모두 선택해주세요.')
            return
            
        default_out = os.path.join(os.path.dirname(self.file1), "비교결과_보고서.xlsx")
        out_file, _ = QFileDialog.getSaveFileName(self, '결과 저장 경로 선택', default_out, 'Excel files (*.xlsx)')
        
        if out_file:
            success, msg = compare_excels_logic(self.file1, self.file2, out_file)
            if success:
                QMessageBox.information(self, '성공', msg)
            else:
                QMessageBox.critical(self, '오류', msg)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    ex = ExcelComparatorApp()
    ex.show()
    sys.exit(app.exec())
