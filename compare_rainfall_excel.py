import pandas as pd
import numpy as np
import os
import argparse

def compare_excels(file1_path, file2_path, output_path):
    print(f"Comparing:\n  1: {file1_path}\n  2: {file2_path}")
    
    xl1 = pd.ExcelFile(file1_path)
    xl2 = pd.ExcelFile(file2_path)
    
    common_sheets = sorted(list(set(xl1.sheet_names) & set(xl2.sheet_names)))
    print(f"Common sheets found: {len(common_sheets)}")
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        summary_data = []
        
        for sheet in common_sheets:
            print(f"Processing sheet: {sheet}")
            df1 = pd.read_excel(xl1, sheet_name=sheet)
            df2 = pd.read_excel(xl2, sheet_name=sheet)
            
            # Find year column
            # User says 1st column is year. Let's find it by position or name.
            year_col1 = df1.columns[0] if "Year" not in df1.columns else "Year"
            year_col2 = df2.columns[0] if "Year" not in df2.columns else "Year"
            
            # Rename for consistency
            df1 = df1.rename(columns={year_col1: "Year"})
            df2 = df2.rename(columns={year_col2: "Year"})
            
            # Ensure Year is integer
            df1["Year"] = pd.to_numeric(df1["Year"], errors='coerce').fillna(0).astype(int)
            df2["Year"] = pd.to_numeric(df2["Year"], errors='coerce').fillna(0).astype(int)
            
            # Merge on Year
            # Use inner join to only compare overlapping years
            merged = pd.merge(df1, df2, on="Year", suffixes=('_f1', '_f2'))
            
            # Identify columns to compare (numeric columns excluding Year)
            # Find common columns between the two (excluding Year)
            cols1 = [c for c in df1.columns if c != "Year"]
            cols2 = [c for c in df2.columns if c != "Year"]
            # Convert column names to string for comparison if they are numbers
            cols1_str = [str(c) for c in cols1]
            cols2_str = [str(c) for c in cols2]
            
            common_cols = sorted(list(set(cols1_str) & set(cols2_str)))
            
            diff_records = []
            total_diffs = 0
            
            for col in common_cols:
                # Handle numeric names vs string names in original dfs
                # Map back to original column names
                orig_col1 = next((c for c in cols1 if str(c) == col), None)
                orig_col2 = next((c for c in cols2 if str(c) == col), None)
                
                if orig_col1 is None or orig_col2 is None:
                    continue
                
                f1_col = f"{orig_col1}_f1"
                f2_col = f"{orig_col2}_f2"
                
                # Calculate absolute difference
                diff = (merged[f1_col] - merged[f2_col]).abs()
                
                # Check for differences (with a small epsilon for floating point)
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
                # Reorder columns
                final_diff_df = final_diff_df[["Year", "Column", "File1_Val", "File2_Val", "Diff"]]
                final_diff_df.to_excel(writer, sheet_name=sheet, index=False)
                summary_data.append({"Sheet": sheet, "Status": "Differences Found", "Diff Count": total_diffs})
            else:
                summary_data.append({"Sheet": sheet, "Status": "Identical", "Diff Count": 0})
        
        # Add sheets that were in one file but not the other
        only_in_f1 = set(xl1.sheet_names) - set(xl2.sheet_names)
        only_in_f2 = set(xl2.sheet_names) - set(xl1.sheet_names)
        
        for s in only_in_f1:
            summary_data.append({"Sheet": s, "Status": "Only in File 1", "Diff Count": "N/A"})
        for s in only_in_f2:
            summary_data.append({"Sheet": s, "Status": "Only in File 2", "Diff Count": "N/A"})
            
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name="Summary", index=False)
        
    print(f"Comparison complete. Results saved to: {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare two rainfall Excel files.")
    parser.add_argument("--f1", help="Path to first Excel file", default=r"C:\Workspace\test\검증\3.1 임의시간최대강우_기상청60min적용.xlsx")
    parser.add_argument("--f2", help="Path to second Excel file", default=r"C:\Workspace\test\검증\R3_임의시간최대_60min.xlsx")
    parser.add_argument("--out", help="Path to output result file", default=r"C:\Workspace\test\검증\비교결과_보고서.xlsx")
    
    args = parser.parse_args()
    
    compare_excels(args.f1, args.f2, args.out)
