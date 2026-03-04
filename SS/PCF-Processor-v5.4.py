#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCF Processor Complete v5.4 - PCF file processor with detailed error logging
Creates three Excel tables from PCF files:
1. ALL PIPELINE-REFERENCE block data (dynamically determined columns)
2. INSTRUMENT information (TAG fields)
3. PipeLengthFromPCF - pipe lengths, SUPPORT, FLANGE, VALVE by LINEID/SIZE/UNIT

UPDATED v5.4:
- ERROR LOGGING system with 4 validation checks
- Multiple PIPELINE-REFERENCE in one file detection
- Empty PCF file detection
- Duplicate PIPELINE-REFERENCE across files detection
- PIPELINE-REFERENCE name match validation with PCF filename
- All errors exported to Excel error log
- NEW: Each error = separate row (one PIPELINE-REFERENCE per row for Multiple errors)
- NEW: File access error handling with retry mechanism
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
        self.error_log = []  # Collect all errors during processing
        self.seen_pipeline_refs = {}  # Track PIPELINE-REF across files for duplication check
    
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
    
    def add_error(self, pcf_name, pipeline_ref, error_type):
        """Add error to error log - one row per PIPELINE-REFERENCE"""
        self.error_log.append({
            'PCF_File': pcf_name,
            'PIPELINE-REFERENCE': pipeline_ref,
            'Error_Type': error_type
        })
    
    def extract_pipeline_refs_from_filename(self, filename):
        """Extract potential PIPELINE-REFERENCE from filename"""
        # Remove extension and revision info
        name = os.path.splitext(filename)[0]  # Remove .pcf
        # Remove -Rev.XX suffix
        name = re.sub(r'-Rev\.\d+$', '', name)
        return name
    
    def validate_pcf_file(self, file_path, filename):
        """Validate PCF file for errors"""
        try:
            for encoding in ['latin1', 'utf-8', 'cp1252', 'iso-8859-1']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                self.add_error(filename, "N/A", "Read Error")
                return None
        except Exception as e:
            self.add_error(filename, "N/A", "Read Error")
            return None
        
        # Check 1: Empty file
        if not content.strip():
            self.add_error(filename, "N/A", "Empty")
            return None
        
        lines = content.split('\n')
        
        # Check 2: Multiple PIPELINE-REFERENCE (without indentation)
        pipeline_refs_no_indent = []
        for idx, line in enumerate(lines):
            if line and not line[0].isspace() and line.strip().startswith('PIPELINE-REFERENCE'):
                pipeline_refs_no_indent.append(line.strip())
        
        if len(pipeline_refs_no_indent) > 1:
            # Create separate error row for EACH PIPELINE-REFERENCE found
            for ref in pipeline_refs_no_indent:
                ref_value = ref.split(None, 1)[1] if len(ref.split(None, 1)) > 1 else "UNKNOWN"
                self.add_error(filename, ref_value, "Multiple PIPELINE-REFERENCE")
        
        # Check 3: Duplicate PIPELINE-REFERENCE across files
        for ref in pipeline_refs_no_indent:
            ref_value = ref.split(None, 1)[1] if len(ref.split(None, 1)) > 1 else "UNKNOWN"
            if ref_value in self.seen_pipeline_refs:
                self.add_error(filename, ref_value, "Duplication of PIPELINE-REFERENCE")
            else:
                self.seen_pipeline_refs[ref_value] = filename
        
        # Check 4: PIPELINE-REFERENCE match with filename
        expected_from_filename = self.extract_pipeline_refs_from_filename(filename)
        
        for ref in pipeline_refs_no_indent:
            ref_value = ref.split(None, 1)[1] if len(ref.split(None, 1)) > 1 else "UNKNOWN"
            # Check if ref_value is contained in filename (or vice versa)
            if ref_value not in expected_from_filename and expected_from_filename not in ref_value:
                self.add_error(filename, ref_value, "PIPELINE-REFERENCE doesn't match PCF name")
        
        return content
    
    def parse_pcf_file(self, file_path):
        """
        Parse PCF file for attributes and instruments
        Returns tuple: (pipeline_data, instruments)
        """
        try:
            for encoding in ['latin1', 'utf-8', 'cp1252', 'iso-8859-1']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                self.logger.error(f"Cannot read file: {file_path}")
                return None, None
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {e}")
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
        """
        Parse PCF file to calculate pipe lengths and count components
        
        LOGIC v5.1:
        - PIPELINE-REFERENCE WITHOUT indentation = start of new pipeline
        - PIPELINE-REFERENCE WITH indentation = just a reference (ignored!)
        """
        try:
            for encoding in ['latin1', 'utf-8', 'cp1252', 'iso-8859-1']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        lines = f.readlines()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                self.logger.error(f"Cannot read file: {file_path}")
                return []
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {e}")
            return []
        
        # Extract global UNITS-BORE
        global_unit = None
        for line in lines[:20]:
            stripped = line.strip()
            if stripped.startswith('UNITS-BORE'):
                parts = stripped.split()
                if len(parts) > 1:
                    global_unit = parts[1]
                break
        
        # STAGE 1: Find all PIPELINE-REFERENCE blocks (WITHOUT indentation only!)
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
        
        # STAGE 2: Process each PIPELINE block
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
            
            # STAGE 3: Find all segments in this PIPELINE block
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
            
            # STAGE 4: Process each segment
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
            
            # STAGE 5: Group by SIZE and create output records
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
                    # Create empty sheet
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
                
                # Successfully saved
                self.logger.info(f"Successfully saved: {os.path.basename(filepath)}")
                return True
                
            except PermissionError as e:
                retry_count += 1
                self.logger.warning(f"File access error on {os.path.basename(filepath)}: {e}")
                
                if retry_count < max_retries:
                    # File is open, ask user
                    result = messagebox.askyesnocancel(
                        "File is Open",
                        f"The file '{os.path.basename(filepath)}' is currently open in another application.\n\n"
                        f"Please close the file and click 'Yes' to retry.\n"
                        f"Click 'No' to skip this file.\n"
                        f"Click 'Cancel' to abort processing.",
                        icon=messagebox.WARNING
                    )
                    
                    if result is None:  # Cancel
                        self.logger.error(f"User cancelled processing due to file access error")
                        raise Exception("Processing cancelled by user due to file access error")
                    elif result is False:  # No - skip
                        self.logger.warning(f"Skipped saving {os.path.basename(filepath)}")
                        return False
                    else:  # Yes - retry
                        time.sleep(1)  # Wait 1 second before retrying
                        continue
                else:
                    # Max retries exceeded
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
    
    def process_folder(self, folder_path, progress_callback=None, save_paths=None, include_continuation=False):
        """
        Process all PCF files in folder and create three Excel tables + error log
        """
        # Reset error log for this run
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
                row_data = {
                    'File_Name': filename,
                    'Full_Path': file_path
                }
                for key in sorted(all_pipeline_keys):
                    row_data[key] = pipeline_data.get(key, '')
                
                table1_data.append(row_data)
                
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
            df1 = pd.DataFrame(table1_data) if table1_data else pd.DataFrame()
            df2 = pd.DataFrame(table2_data) if table2_data else pd.DataFrame()
            df3 = pd.DataFrame(table3_data) if table3_data else pd.DataFrame()
            df_errors = pd.DataFrame(self.error_log) if self.error_log else pd.DataFrame()
            
            if save_paths:
                output_path1 = save_paths.get('pipeline')
                output_path2 = save_paths.get('instruments')
                output_path3 = save_paths.get('lengths')
                output_path_errors = save_paths.get('errors')
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_path1 = os.path.join(folder_path, f'PCF_Pipeline_Attributes_{timestamp}.xlsx')
                output_path2 = os.path.join(folder_path, f'PCF_Instruments_{timestamp}.xlsx')
                output_path3 = os.path.join(folder_path, f'PCF_PipeLengthFromPCF_{timestamp}.xlsx')
                output_path_errors = os.path.join(folder_path, f'PCF_Errors_{timestamp}.xlsx')
            
            # Save files with retry mechanism
            self._save_excel_with_retry(df1, output_path1, 'Pipeline_Attributes')
            self._save_excel_with_retry(df2, output_path2, 'Instruments')
            self._save_excel_with_retry(df3, output_path3, 'PipeLengthFromPCF')
            self._save_excel_with_retry(df_errors, output_path_errors, 'Errors')
            
            total_instruments = len([x for x in table2_data if x['INSTRUMENT']])
            total_length_records = len(table3_data)
            
            self.logger.info(f"Processing completed successfully!")
            self.logger.info(f"Processed files: {processed_files}")
            self.logger.info(f"Errors: {errors}")
            self.logger.info(f"Instruments found: {total_instruments}")
            self.logger.info(f"Pipe length records: {total_length_records}")
            self.logger.info(f"Validation errors: {len(self.error_log)}")
            
            message = (f"Processing completed!\n\n"
                      f"Statistics:\n"
                      f"• Processed files: {processed_files}\n"
                      f"• Errors: {errors}\n"
                      f"• Instruments found: {total_instruments}\n"
                      f"• Pipe length records: {total_length_records}\n"
                      f"• Pipeline parameters found: {len(all_pipeline_keys)}\n"
                      f"• Include CONTINUATION: {'YES' if include_continuation else 'NO'}\n"
                      f"• Validation errors found: {len(self.error_log)}\n\n"
                      f"Files created:\n"
                      f"• {os.path.basename(output_path1)}\n"
                      f"• {os.path.basename(output_path2)}\n"
                      f"• {os.path.basename(output_path3)}\n"
                      f"• {os.path.basename(output_path_errors)}")
            
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
    
    root = tk.Tk()
    root.title("PCF File Processor v5.4")
    root.geometry("700x620")
    root.resizable(False, False)
    
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (700 // 2)
    y = (root.winfo_screenheight() // 2) - (620 // 2)
    root.geometry(f"700x620+{x}+{y}")
    
    title_label = tk.Label(root, text="PCF File Processor v5.4", font=("Arial", 16, "bold"))
    title_label.pack(pady=20)
    
    description_text = """PCF File Processor with Complete Analysis
    
Creates FOUR Excel files:
1. PIPELINE-REFERENCE attributes (dynamic columns)
2. INSTRUMENT data (TAG fields)
3. PipeLengthFromPCF - pipe lengths and components
4. ERROR LOG - validation results

✓ Error Log Format (Each row = one error):
   • PCF_File: Filename
   • PIPELINE-REFERENCE: The problematic reference
   • Error_Type: Empty / Multiple PIPELINE-REFERENCE / 
               Duplication / doesn't match PCF name
   
✓ Validation Checks:
   • Empty PCF files detection
   • Multiple PIPELINE-REFERENCE per file
   • Duplicate PIPELINE-REFERENCE across files
   • PIPELINE-REFERENCE vs filename mismatch
   
✓ Enhanced Features:
   • PIPELINE-REFERENCE without indentation only
   • Multiple pipelines in one file support
   • Dynamic parsing without hard boundaries
   • All errors to separate Excel sheet
   • File access error handling with retry mechanism"""
    
    description = tk.Label(root, text=description_text, justify="left", wraplength=650, font=("Arial", 9))
    description.pack(pady=10, padx=20)
    
    include_continuation_var = tk.BooleanVar(value=False)
    continuation_frame = tk.Frame(root)
    continuation_frame.pack(pady=5)
    
    continuation_check = tk.Checkbutton(
        continuation_frame,
        text="Include blocks with CONTINUATION (default: excluded)",
        variable=include_continuation_var,
        font=("Arial", 10)
    )
    continuation_check.pack()
    
    def select_and_process():
        folder_path = filedialog.askdirectory(title="Select folder with PCF files")
        if not folder_path:
            return
        
        pipeline_save_path = filedialog.asksaveasfilename(
            title="Save pipeline attributes file as...",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile="PCF_Pipeline_Attributes"
        )
        if not pipeline_save_path:
            return
        
        instrument_save_path = filedialog.asksaveasfilename(
            title="Save instruments file as...",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile="PCF_Instruments"
        )
        if not instrument_save_path:
            return
        
        lengths_save_path = filedialog.asksaveasfilename(
            title="Save pipe lengths file as...",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile="PCF_PipeLengthFromPCF"
        )
        if not lengths_save_path:
            return
        
        errors_save_path = filedialog.asksaveasfilename(
            title="Save error log file as...",
            defaultextension=".xlsx",
            filetypes=[("Excel files", "*.xlsx")],
            initialfile="PCF_Errors"
        )
        if not errors_save_path:
            return
        
        progress_window = ProgressWindow(root)
        
        def progress_callback(current, total, status):
            progress_window.update_progress(current, total, status)
        
        try:
            success = processor.process_folder(
                folder_path,
                progress_callback,
                save_paths={
                    'pipeline': pipeline_save_path,
                    'instruments': instrument_save_path,
                    'lengths': lengths_save_path,
                    'errors': errors_save_path
                },
                include_continuation=include_continuation_var.get()
            )
            
            progress_window.close()
            
            if success:
                if messagebox.askyesno("Open folder?", "Do you want to open results folder?"):
                    os.startfile(os.path.dirname(pipeline_save_path))
                
                root.quit()
                root.destroy()
        
        except Exception as e:
            progress_window.close()
            messagebox.showerror("Error", f"An error occurred: {e}")
    
    select_button = tk.Button(
        root,
        text="Select Folder and Start Processing",
        command=select_and_process,
        font=("Arial", 12),
        bg="#4CAF50",
        fg="white",
        padx=20,
        pady=12
    )
    select_button.pack(pady=15)
    
    info_label = tk.Label(
        root,
        text="Supported formats: .pcf • Recursive folder search • Automatic error detection and logging",
        font=("Arial", 9),
        fg="gray"
    )
    info_label.pack(side="bottom", pady=10)
    
    root.mainloop()


if __name__ == "__main__":
    main()
