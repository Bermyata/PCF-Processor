#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCF Processor Complete v5.5 - PCF file processor with selective report generation
Creates Excel tables from PCF files based on user selection:
1. Pipeline Attributes - all PIPELINE-REFERENCE block data
2. Instruments - TAG fields from INSTRUMENT blocks
3. PipeLengthFromPCF - pipe lengths, SUPPORT, FLANGE, VALVE by LINEID/SIZE/UNIT
4. PCF_Errors - validation error log

UPDATED v5.5:
- NEW UI: Folder selection + selective report checkboxes
- Generate reports only for selected types
- Choose save location only for selected reports
- CONTINUATION checkbox
- START/GENERATE button workflow
"""

import os
import pandas as pd
import numpy as np
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import re
import logging
from datetime import datetime
import math
import time


class PCFProcessor:

    def __init__(self):
        self.setup_logging()
        self.error_log = []
        self.seen_pipeline_refs = {}  # Теперь храним {ref_value: {'filename': ..., 'full_path': ...}}    
    
    def setup_logging(self):
        """Setup logging"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pcf_processor.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _read_pcf_file(self, file_path, filename=None):
        """
        Read PCF file with multiple encoding attempts
        
        Args:
            file_path: Path to PCF file
            filename: Optional filename for error logging
            
        Returns:
            str: File content or None if read failed
        """
        if filename is None:
            filename = os.path.basename(file_path)
        
        encodings = ['latin1', 'utf-8', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    content = f.read()
                return content
            except UnicodeDecodeError:
                continue
            except Exception as e:
                self.logger.error(f"Error reading file {filename}: {e}")
                return None
        
        # All encodings failed
        self.logger.error(f"Cannot read file {filename} with any supported encoding")
        return None
    
    def add_error(self, pcf_name, pipeline_ref, error_type, full_path=None):
        """Add error to error log - one row per PIPELINE-REFERENCE"""
        self.error_log.append({
            'PCF_File': pcf_name,
            'PIPELINE-REFERENCE': pipeline_ref,
            'Error_Type': error_type,
            'Full_Path': full_path if full_path else ''
        })
    
    def extract_pipeline_refs_from_filename(self, filename):
        """Extract potential PIPELINE-REFERENCE from filename"""
        name = os.path.splitext(filename)[0]
        name = re.sub(r'-Rev\.\d+$', '', name)
        return name
    
    def validate_pcf_file(self, file_path, filename):
        """Validate PCF file for errors"""
        content = self._read_pcf_file(file_path, filename)
        
        if content is None:
            self.add_error(filename, "N/A", "Read Error", full_path=file_path)
            return None
        
        if not content.strip():
            self.add_error(filename, "N/A", "Empty", full_path=file_path)
            return None
        
        lines = content.split('\n')
        
        pipeline_refs_no_indent = []
        for idx, line in enumerate(lines):
            if line and not line[0].isspace() and line.strip().startswith('PIPELINE-REFERENCE'):
                pipeline_refs_no_indent.append(line.strip())
        
        if len(pipeline_refs_no_indent) > 1:
            for ref in pipeline_refs_no_indent:
                ref_value = ref.split(None, 1)[1] if len(ref.split(None, 1)) > 1 else "UNKNOWN"
                self.add_error(filename, ref_value, "Multiple PIPELINE-REFERENCE", full_path=file_path)
        
        for ref in pipeline_refs_no_indent:
            ref_value = ref.split(None, 1)[1] if len(ref.split(None, 1)) > 1 else "UNKNOWN"
            if ref_value in self.seen_pipeline_refs:
                # Дубликат найден - добавляем ошибку для ТЕКУЩЕГО файла
                self.add_error(filename, ref_value, "Duplication of PIPELINE-REFERENCE", full_path=file_path)
                
                # Добавляем ошибку для ПЕРВОГО файла (если еще не добавляли)
                first_file_info = self.seen_pipeline_refs[ref_value]
                first_file = first_file_info['filename']
                first_path = first_file_info['full_path']
                
                already_logged = any(
                    err['PCF_File'] == first_file and 
                    err['PIPELINE-REFERENCE'] == ref_value and 
                    err['Error_Type'] == "Duplication of PIPELINE-REFERENCE"
                    for err in self.error_log
                )
                if not already_logged:
                    self.add_error(first_file, ref_value, "Duplication of PIPELINE-REFERENCE", full_path=first_path)
            else:
                # Сохраняем и имя файла, и полный путь
                self.seen_pipeline_refs[ref_value] = {
                    'filename': filename,
                    'full_path': file_path
                }
        
        expected_from_filename = self.extract_pipeline_refs_from_filename(filename)
        
        for ref in pipeline_refs_no_indent:
            ref_value = ref.split(None, 1)[1] if len(ref.split(None, 1)) > 1 else "UNKNOWN"
            if ref_value not in expected_from_filename and expected_from_filename not in ref_value:
                self.add_error(filename, ref_value, "PIPELINE-REFERENCE doesn't match PCF name", full_path=file_path)
        
        return content
    
    def parse_pcf_file(self, file_path):
        """Parse PCF file for attributes and instruments"""
        content = self._read_pcf_file(file_path)
        
        if content is None:
            return None, None
        
        pipeline_data = {}
        instruments = []
        lines = content.split('\n')
        in_pipeline_block = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if line.startswith('PIPELINE-REFERENCE'):
                in_pipeline_block = True
                parts = line.split(None, 1)
                if len(parts) > 1:
                    pipeline_data['PIPELINE-REFERENCE'] = parts[1]
                continue
            
            if in_pipeline_block:
                component_blocks = [
                    'FLANGE-BLIND', 'GASKET', 'BOLT', 'INSTRUMENT', 'PIPE', 'ELBOW',
                    'TEE', 'REDUCER', 'VALVE', 'OLET', 'SUPPORT', 'COMPONENT-IDENTIFIER',
                    'END-POINT', 'CENTRE-POINT', 'SKEY', 'MATERIAL-IDENTIFIER'
                ]
                if any(line.startswith(block) for block in component_blocks):
                    break
                
                if ' ' in line:
                    parts = line.split(None, 1)
                    if len(parts) == 2:
                        pipeline_data[parts[0]] = parts[1]
        
        instrument_pattern = r'INSTRUMENT\s*\n(.*?)(?=\n[A-Z-]+(?:\s|\n)|\nEND-POSITION|\nCONNECTION-REFERENCE|\nMATERIALS|\Z)'
        instrument_blocks = re.finditer(instrument_pattern, content, re.DOTALL)
        
        for block in instrument_blocks:
            block_content = block.group(1)
            tag_match = re.search(r'TAG\s+(.+)', block_content, re.MULTILINE)
            if tag_match:
                instruments.append(tag_match.group(1).strip())
        
        return pipeline_data, instruments
    
    def parse_pcf_for_lengths(self, file_path, include_continuation=False):
        """Parse PCF file to calculate pipe lengths and count components"""
        content = self._read_pcf_file(file_path)
        
        if content is None:
            return []
        
        lines = content.split('\n')
        
        global_unit = None
        for line in lines[:20]:
            stripped = line.strip()
            if stripped.startswith('UNITS-BORE'):
                parts = stripped.split()
                if len(parts) > 1:
                    global_unit = parts[1]
                break
        
        pipeline_blocks = []
        for idx, line in enumerate(lines):
            if line and not line[0].isspace() and line.strip().startswith('PIPELINE-REFERENCE'):
                if pipeline_blocks:
                    pipeline_blocks[-1]['end'] = idx
                
                parts = line.strip().split(None, 1)
                lineid = parts[1] if len(parts) > 1 else 'UNKNOWN'
                
                pipeline_blocks.append({
                    'lineid': lineid,
                    'start': idx,
                    'end': None
                })
        
        if pipeline_blocks and pipeline_blocks[-1]['end'] is None:
            pipeline_blocks[-1]['end'] = len(lines)
        
        output = []
        
        for block in pipeline_blocks:
            lineid = block['lineid']
            block_start = block['start']
            block_end = block['end']
            
            unit = global_unit
            for line in lines[block_start:min(block_start + 30, block_end)]:
                stripped = line.strip()
                if stripped.startswith('UNITS-BORE'):
                    parts = stripped.split()
                    if len(parts) > 1:
                        unit = parts[1]
                    break
            
            if not unit:
                self.logger.warning(f"UNIT not found for PIPELINE {lineid}")
                unit = 'INCH'
            
            segments = []
            for idx in range(block_start + 1, block_end):
                line = lines[idx]
                if (
                    line
                    and not line[0].isspace()
                    and line.strip()
                    and not line.strip().startswith('PIPELINE-REFERENCE')
                ):
                    segments.append({
                        'type': line.strip(),
                        'start_idx': idx
                    })
            
            results = []
            for i, seg in enumerate(segments):
                start = seg['start_idx']
                end = segments[i + 1]['start_idx'] if i + 1 < len(segments) else block_end
                
                seg_lines = lines[start:end]
                seg_type = seg['type']
                
                has_cont = any('CONTINUATION' in line for line in seg_lines)
                if has_cont and not include_continuation:
                    continue
                
                endpoints = []
                for line in seg_lines:
                    stripped = line.strip()
                    if stripped.startswith('END-POINT') or stripped.startswith('CO-ORDS'):
                        parts = stripped.split()
                        if len(parts) >= 5:
                            try:
                                x = float(parts[1])
                                y = float(parts[2])
                                z = float(parts[3])
                                size = float(parts[4]) if parts[4].replace('.', '').replace('-', '').isdigit() else None
                                endpoints.append({'x': x, 'y': y, 'z': z, 'size': size})
                            except (ValueError, IndexError):
                                pass
                
                if len(endpoints) >= 2:
                    ep1 = endpoints[0]
                    ep2 = endpoints[1]
                    
                    size1 = ep1['size'] if ep1['size'] is not None else 0
                    size2 = ep2['size'] if ep2['size'] is not None else 0
                    final_size = max(size1, size2)
                    
                    if final_size == 0:
                        continue
                    
                    dx = ep2['x'] - ep1['x']
                    dy = ep2['y'] - ep1['y']
                    dz = ep2['z'] - ep1['z']
                    
                    if seg_type in ['ELBOW', 'BEND']:
                        length = (abs(dx) + abs(dy) + abs(dz)) / 4 * math.pi
                    elif seg_type == 'TEE':
                        base_length = math.sqrt(dx**2 + dy**2 + dz**2)
                        length = base_length + base_length / 2
                    else:
                        length = math.sqrt(dx**2 + dy**2 + dz**2)
                    
                    results.append({
                        'type': seg_type,
                        'size': final_size,
                        'length': length,
                        'is_support': 1 if seg_type == 'SUPPORT' else 0,
                        'is_flange': 1 if seg_type == 'FLANGE' else 0,
                        'is_valve': 1 if seg_type == 'VALVE' else 0
                    })
                
                elif len(endpoints) == 1 and seg_type == 'SUPPORT':
                    size = endpoints[0]['size']
                    if size:
                        results.append({
                            'type': seg_type,
                            'size': size,
                            'length': 0,
                            'is_support': 1,
                            'is_flange': 0,
                            'is_valve': 0
                        })
            
            if not results:
                continue
            
            result_df = pd.DataFrame(results)
            grouped = result_df.groupby('size').agg({
                'length': 'sum',
                'is_support': 'sum',
                'is_flange': 'sum',
                'is_valve': 'sum'
            }).reset_index()
            
            for _, row in grouped.iterrows():
                output.append({
                    'LINEID': lineid,
                    'SIZE': row['size'],
                    'UNIT': unit,
                    'LENGTH': math.ceil(row['length']),
                    'SUPPORT_QTY': int(row['is_support']),
                    'FLANGE_QTY': int(row['is_flange']),
                    'VALVE_QTY': int(row['is_valve'])
                })
        
        return output
    
    def _save_excel_with_retry(self, df, filepath, sheetname, max_retries=3):
        """Save DataFrame to Excel with retry mechanism for file access errors"""
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if df.empty:
                    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                        pd.DataFrame({'Status': ['No data']}).to_excel(writer, index=False, sheet_name=sheetname)
                else:
                    with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                        df.to_excel(writer, index=False, sheet_name=sheetname)
                        worksheet = writer.sheets[sheetname]
                        
                        for column in worksheet.columns:
                            max_length = 0
                            column_letter = column[0].column_letter
                            for cell in column:
                                try:
                                    if len(str(cell.value)) > max_length:
                                        max_length = len(str(cell.value))
                                except:
                                    pass
                            adjusted_width = min(max_length + 2, 50)
                            worksheet.column_dimensions[column_letter].width = adjusted_width
                
                self.logger.info(f"Successfully saved: {os.path.basename(filepath)}")
                return True
                
            except PermissionError as e:
                retry_count += 1
                self.logger.warning(f"File access error on {os.path.basename(filepath)}: {e}")
                
                if retry_count < max_retries:
                    result = messagebox.askyesnocancel(
                        "File is Open",
                        f"The file '{os.path.basename(filepath)}' is currently open in another application.\n\n"
                        f"Please close the file and click 'Yes' to retry.\n"
                        f"Click 'No' to skip this file.\n"
                        f"Click 'Cancel' to abort processing.",
                        icon=messagebox.WARNING
                    )
                    
                    if result is None:
                        self.logger.error(f"User cancelled processing due to file access error")
                        raise Exception("Processing cancelled by user due to file access error")
                    elif result is False:
                        self.logger.warning(f"Skipped saving {os.path.basename(filepath)}")
                        return False
                    else:
                        time.sleep(1)
                        continue
                else:
                    messagebox.showerror(
                        "Error",
                        f"Failed to save '{os.path.basename(filepath)}' after {max_retries} attempts.\n\n"
                        f"Please close the file and try processing again."
                    )
                    raise
            
            except Exception as e:
                self.logger.error(f"Error saving {filepath}: {e}")
                messagebox.showerror("Error", f"Error saving {os.path.basename(filepath)}:\n{e}")
                raise
        
        return False
    
    def process_folder(self, folder_path, progress_callback=None, save_paths=None, include_continuation=False, generate_reports=None):
        """
        Process all PCF files in folder and create selected Excel tables
        
        Args:
            generate_reports: dict with report names as keys and boolean values
                             {'pipeline': True, 'instruments': False, 'lengths': True, 'errors': True}
        """
        if generate_reports is None:
            generate_reports = {'pipeline': True, 'instruments': True, 'lengths': True, 'errors': True}
        
        self.error_log = []
        self.seen_pipeline_refs = {}
        
        pcf_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.pcf'):
                    pcf_files.append(os.path.join(root, file))
        
        if not pcf_files:
            messagebox.showwarning("Warning", "No PCF files found in selected folder!")
            return False
        
        self.logger.info(f"Found {len(pcf_files)} PCF files to process")
        self.logger.info(f"Include CONTINUATION: {'YES' if include_continuation else 'NO'}")
        self.logger.info(f"Generate reports: {generate_reports}")
        
        table1_data = []
        table2_data = []
        table3_data = []
        all_pipeline_keys = set()
        
        processed_files = 0
        errors = 0
        
        # Validation pass
        self.logger.info("Validation pass - checking for errors...")
        for i, file_path in enumerate(pcf_files):
            if progress_callback:
                progress_callback(i, len(pcf_files) * 3, 
                                f"Validation {i+1}/{len(pcf_files)}: {os.path.basename(file_path)}")
            
            self.validate_pcf_file(file_path, os.path.basename(file_path))
        
        # First pass - determine all pipeline keys
        self.logger.info("First pass - analyzing file structure...")
        for i, file_path in enumerate(pcf_files):
            if progress_callback:
                progress_callback(len(pcf_files) + i, len(pcf_files) * 3,
                                f"Analysis {i+1}/{len(pcf_files)}: {os.path.basename(file_path)}")
            
            pipeline_data, _ = self.parse_pcf_file(file_path)
            if pipeline_data:
                all_pipeline_keys.update(pipeline_data.keys())
        
        self.logger.info(f"Found {len(all_pipeline_keys)} unique parameters across all files")
        
        # Second pass - collect data
        self.logger.info("Second pass - collecting data...")
        for i, file_path in enumerate(pcf_files):
            if progress_callback:
                progress_callback(len(pcf_files) * 2 + i, len(pcf_files) * 3,
                                f"Processing {i+1}/{len(pcf_files)}: {os.path.basename(file_path)}")
            
            filename = os.path.basename(file_path)
            pipeline_data, instruments = self.parse_pcf_file(file_path)
            
            if pipeline_data is not None:
                if generate_reports['pipeline']:
                    row_data = {
                        'File_Name': filename,
                        'Full_Path': file_path
                    }
                    for key in sorted(all_pipeline_keys):
                        row_data[key] = pipeline_data.get(key, '')
                    table1_data.append(row_data)
                
                if generate_reports['instruments']:
                    if instruments:
                        for instrument in instruments:
                            table2_data.append({
                                'File_Name': filename,
                                'Full_Path': file_path,
                                'INSTRUMENT': instrument
                            })
                    else:
                        table2_data.append({
                            'File_Name': filename,
                            'Full_Path': file_path,
                            'INSTRUMENT': ''
                        })
                
                processed_files += 1
            else:
                errors += 1
                self.logger.warning(f"Cannot process file: {file_path}")
            
            # Table 3 - PipeLengthFromPCF
            if generate_reports['lengths']:
                length_data = self.parse_pcf_for_lengths(file_path, include_continuation)
                for record in length_data:
                    table3_data.append({
                        'Name': filename,
                        'LINEID': record['LINEID'],
                        'SIZE': record['SIZE'],
                        'UNIT': record['UNIT'],
                        'LENGTH': record['LENGTH'],
                        'SUPPORT QTY': record['SUPPORT_QTY'],
                        'FLANGE QTY': record['FLANGE_QTY'],
                        'VALVE QTY': record['VALVE_QTY'],
                        'Folder Path': os.path.dirname(file_path)
                    })
        
        # Create DataFrames and save to Excel
        try:
            created_files = []
            
            # Save only selected reports
            if generate_reports['pipeline']:
                df1 = pd.DataFrame(table1_data) if table1_data else pd.DataFrame()
                output_path1 = save_paths.get('pipeline')
                self._save_excel_with_retry(df1, output_path1, 'Pipeline_Attributes')
                created_files.append(os.path.basename(output_path1))
            
            if generate_reports['instruments']:
                df2 = pd.DataFrame(table2_data) if table2_data else pd.DataFrame()
                output_path2 = save_paths.get('instruments')
                self._save_excel_with_retry(df2, output_path2, 'Instruments')
                created_files.append(os.path.basename(output_path2))
            
            if generate_reports['lengths']:
                df3 = pd.DataFrame(table3_data) if table3_data else pd.DataFrame()
                output_path3 = save_paths.get('lengths')
                self._save_excel_with_retry(df3, output_path3, 'PipeLengthFromPCF')
                created_files.append(os.path.basename(output_path3))
            
            if generate_reports['errors']:
                df_errors = pd.DataFrame(self.error_log) if self.error_log else pd.DataFrame()
                output_path_errors = save_paths.get('errors')
                self._save_excel_with_retry(df_errors, output_path_errors, 'Errors')
                created_files.append(os.path.basename(output_path_errors))
            
            total_instruments = len([x for x in table2_data if x['INSTRUMENT']]) if table2_data else 0
            total_length_records = len(table3_data) if table3_data else 0
            
            self.logger.info(f"Processing completed successfully!")
            self.logger.info(f"Processed files: {processed_files}")
            self.logger.info(f"Errors: {errors}")
            
            message = (f"Processing completed!\n\n"
                      f"Statistics:\n"
                      f"• Processed files: {processed_files}\n"
                      f"• Errors: {errors}\n"
                      f"• Instruments found: {total_instruments}\n"
                      f"• Pipe length records: {total_length_records}\n"
                      f"• Pipeline parameters found: {len(all_pipeline_keys)}\n"
                      f"• Validation errors found: {len(self.error_log)}\n\n"
                      f"Files created:\n"
                      f"• {chr(10).join([f'• {f}' for f in created_files])}")
            
            messagebox.showinfo("Success", message)
            return True
        
        except Exception as e:
            self.logger.error(f"Error during processing: {e}")
            messagebox.showerror("Error", f"Error during processing:\n{e}")
            return False


class ProgressWindow:
    def __init__(self, parent):
        self.window = tk.Toplevel(parent)
        self.window.title("Processing files...")
        self.window.geometry("500x180")
        self.window.resizable(False, False)
        self.window.transient(parent)
        self.window.grab_set()
        
        self.window.update_idletasks()
        x = (self.window.winfo_screenwidth() // 2) - (500 // 2)
        y = (self.window.winfo_screenheight() // 2) - (180 // 2)
        self.window.geometry(f"500x180+{x}+{y}")
        
        self.label = tk.Label(self.window, text="Preparing...", pady=10)
        self.label.pack()
        
        self.progress = ttk.Progressbar(self.window, mode='determinate', length=450)
        self.progress.pack(pady=10)
        
        self.status_label = tk.Label(self.window, text="", wraplength=450)
        self.status_label.pack()
    
    def update_progress(self, current, total, status):
        if total > 0:
            percentage = (current / total) * 100
            self.progress['value'] = percentage
            self.label.config(text=f"Progress: {current}/{total} ({percentage:.1f}%)")
            self.status_label.config(text=status)
            self.window.update()
    
    def close(self):
        self.window.destroy()


def main():
    """Main application function"""
    processor = PCFProcessor()
    selected_folder = None
    
    root = tk.Tk()
    root.title("PCF File Processor v5.5")
    root.geometry("700x650")
    root.resizable(False, False)
    
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (700 // 2)
    y = (root.winfo_screenheight() // 2) - (650 // 2)
    root.geometry(f"700x650+{x}+{y}")
    
    title_label = tk.Label(root, text="PCF File Processor v5.5", font=("Arial", 16, "bold"))
    title_label.pack(pady=20)
    
    # Folder selection section
    folder_frame = tk.LabelFrame(root, text="Step 1: Select PCF Folder", font=("Arial", 10, "bold"), padx=10, pady=10)
    folder_frame.pack(padx=20, pady=10, fill=tk.X)
    
    folder_label = tk.Label(folder_frame, text="No folder selected", fg="gray", wraplength=600)
    folder_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
    
    def select_folder():
        nonlocal selected_folder
        selected_folder = filedialog.askdirectory(title="Select folder with PCF files")
        if selected_folder:
            folder_label.config(text=selected_folder, fg="black")
    
    folder_button = tk.Button(folder_frame, text="Browse...", command=select_folder, padx=10)
    folder_button.pack(side=tk.RIGHT, padx=5)
    
    # Reports selection section
    reports_frame = tk.LabelFrame(root, text="Step 2: Select Reports to Generate", font=("Arial", 10, "bold"), padx=10, pady=10)
    reports_frame.pack(padx=20, pady=10, fill=tk.X)
    
    var_pipeline = tk.BooleanVar(value=True)
    var_instruments = tk.BooleanVar(value=True)
    var_lengths = tk.BooleanVar(value=True)
    var_errors = tk.BooleanVar(value=True)
    
    check_pipeline = tk.Checkbutton(reports_frame, text="☑ Pipeline Attributes", variable=var_pipeline, font=("Arial", 10))
    check_pipeline.pack(anchor=tk.W, pady=5)
    
    check_instruments = tk.Checkbutton(reports_frame, text="☑ Instruments", variable=var_instruments, font=("Arial", 10))
    check_instruments.pack(anchor=tk.W, pady=5)
    
    check_lengths = tk.Checkbutton(reports_frame, text="☑ PipeLengthFromPCF", variable=var_lengths, font=("Arial", 10))
    check_lengths.pack(anchor=tk.W, pady=5)
    
    check_errors = tk.Checkbutton(reports_frame, text="☑ PCF_Errors (Validation Log)", variable=var_errors, font=("Arial", 10))
    check_errors.pack(anchor=tk.W, pady=5)
    
    # Options section
    options_frame = tk.LabelFrame(root, text="Step 3: Options", font=("Arial", 10, "bold"), padx=10, pady=10)
    options_frame.pack(padx=20, pady=10, fill=tk.X)
    
    var_continuation = tk.BooleanVar(value=False)
    check_continuation = tk.Checkbutton(
        options_frame,
        text="Include blocks with CONTINUATION (default: excluded)",
        variable=var_continuation,
        font=("Arial", 10)
    )
    check_continuation.pack(anchor=tk.W, pady=5)
    
    # Start button
    def start_processing():
        if not selected_folder:
            messagebox.showwarning("Warning", "Please select a PCF folder first!")
            return
        
        # Check if at least one report is selected
        if not any([var_pipeline.get(), var_instruments.get(), var_lengths.get(), var_errors.get()]):
            messagebox.showwarning("Warning", "Please select at least one report to generate!")
            return
        
        # Ask for save locations only for selected reports
        save_paths = {}
        
        if var_pipeline.get():
            path = filedialog.asksaveasfilename(
                title="Save Pipeline Attributes file as...",
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx")],
                initialfile="PCF_Pipeline_Attributes"
            )
            if not path:
                return
            save_paths['pipeline'] = path
        
        if var_instruments.get():
            path = filedialog.asksaveasfilename(
                title="Save Instruments file as...",
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx")],
                initialfile="PCF_Instruments"
            )
            if not path:
                return
            save_paths['instruments'] = path
        
        if var_lengths.get():
            path = filedialog.asksaveasfilename(
                title="Save PipeLengthFromPCF file as...",
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx")],
                initialfile="PCF_PipeLengthFromPCF"
            )
            if not path:
                return
            save_paths['lengths'] = path
        
        if var_errors.get():
            path = filedialog.asksaveasfilename(
                title="Save Errors log file as...",
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx")],
                initialfile="PCF_Errors"
            )
            if not path:
                return
            save_paths['errors'] = path
        
        # Show progress window
        progress_window = ProgressWindow(root)
        
        def progress_callback(current, total, status):
            progress_window.update_progress(current, total, status)
        
        try:
            generate_reports = {
                'pipeline': var_pipeline.get(),
                'instruments': var_instruments.get(),
                'lengths': var_lengths.get(),
                'errors': var_errors.get()
            }
            
            success = processor.process_folder(
                selected_folder,
                progress_callback,
                save_paths=save_paths,
                include_continuation=var_continuation.get(),
                generate_reports=generate_reports
            )
            
            progress_window.close()
            
            if success:
                if messagebox.askyesno("Open folder?", "Do you want to open results folder?"):
                    folder_for_open = os.path.dirname(list(save_paths.values())[0])
                    os.startfile(folder_for_open)
                
                root.quit()
                root.destroy()
        
        except Exception as e:
            progress_window.close()
            messagebox.showerror("Error", f"An error occurred: {e}")
    
    start_button = tk.Button(
        root,
        text="START / GENERATE",
        command=start_processing,
        font=("Arial", 13, "bold"),
        bg="#4CAF50",
        fg="white",
        padx=30,
        pady=15
    )
    start_button.pack(pady=20)
    
    info_label = tk.Label(
        root,
        text="Supported formats: .pcf • Recursive folder search • Automatic error detection and logging",
        font=("Arial", 8),
        fg="gray"
    )
    info_label.pack(side="bottom", pady=10)
    
    root.mainloop()


if __name__ == "__main__":
    main()
