#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCF Processor v7.1 - PCF file processor with selective report generation

Creates Excel tables from PCF files based on user selection:
1. Pipeline Attributes - all PIPELINE-REFERENCE block data
2. Instruments - TAG fields from INSTRUMENT blocks with coordinates
3. PipeLengthFromPCF - pipe lengths, SUPPORT, FLANGE, VALVE by LINEID/SIZE/UNIT
4. PCF_Errors - validation error log

Features:
- Folder selection + selective report checkboxes
- Generate reports only for selected types
- Choose save location only for selected reports
- CONTINUATION checkbox
- Revision extraction from filenames
- START/GENERATE button workflow
"""

from __future__ import annotations

import logging
import math
import os
import re
import subprocess
import sys
import time
from collections import defaultdict                         # ← NEW
from dataclasses import dataclass, field
from typing import Optional

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Endpoint:
    """3D coordinate with optional bore size."""
    x: float
    y: float
    z: float
    size: Optional[float] = None


@dataclass
class InstrumentData:
    """Instrument tag with optional coordinates."""
    tag: str
    x: str = ''
    y: str = ''
    z: str = ''


@dataclass
class PipeSegmentResult:
    """Aggregated result for one segment."""
    seg_type: str
    size: float
    length: float
    is_support: bool = False
    is_flange: bool = False
    is_valve: bool = False


@dataclass
class RevisionSettings:
    """Settings for revision extraction from filenames."""
    enabled: bool = False
    left_delimiter: str = ''
    right_delimiter: str = ''


@dataclass
class ProcessingResult:
    """Outcome of a full processing run (no UI references)."""
    success: bool
    processed_files: int = 0
    errors: int = 0
    instruments_count: int = 0
    length_records: int = 0
    pipeline_keys_count: int = 0
    validation_errors_count: int = 0
    created_files: list = field(default_factory=list)
    error_message: str = ''


# ---------------------------------------------------------------------------
# Core processor (no UI imports)
# ---------------------------------------------------------------------------

class PCFProcessor:
    """Business-logic layer: reads / validates / parses PCF files."""

    SUPPORTED_ENCODINGS = ('utf-8', 'latin1', 'cp1252', 'iso-8859-1')

    COMPONENT_BLOCK_TYPES = frozenset({
        'FLANGE-BLIND', 'GASKET', 'BOLT', 'INSTRUMENT', 'PIPE', 'ELBOW',
        'TEE', 'REDUCER', 'VALVE', 'OLET', 'SUPPORT', 'COMPONENT-IDENTIFIER',
        'END-POINT', 'CENTRE-POINT', 'SKEY', 'MATERIAL-IDENTIFIER',
        'FLANGE',
    })

    DEFAULT_UNIT = 'INCH'
    _HEADER_SCAN_LINES = 20          # ← NEW: сколько строк сканировать для глобального UNITS-BORE
    _UNIT_SCAN_RANGE = 30            # ← NEW: диапазон поиска UNITS-BORE внутри блока



    def __init__(self) -> None:
        self.logger = self._create_logger()
        self.error_log: list[dict] = []
        self.seen_pipeline_refs: dict[str, dict] = {}
        self.revision = RevisionSettings()

    # -- helpers --------------------------------------------------------

    @staticmethod
    def _create_logger() -> logging.Logger:
        """Return a named logger with file + console handlers."""
        logger = logging.getLogger('pcf_processor')
        if not logger.handlers:
            logger.setLevel(logging.INFO)
            fmt = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            fh = logging.FileHandler('pcf_processor.log', encoding='utf-8')
            fh.setFormatter(fmt)
            sh = logging.StreamHandler()
            sh.setFormatter(fmt)
            logger.addHandler(fh)
            logger.addHandler(sh)
        return logger

    def _get_revision(self, filename: str) -> str:
        """Shortcut: extract revision using current settings."""
        if not self.revision.enabled:
            return ''
        return self.extract_revision_from_filename(
            filename, self.revision.left_delimiter, self.revision.right_delimiter
        )
    
    def _read_pcf_file(self, file_path: str, filename: str | None = None) -> str | None:
        """Read PCF file trying multiple encodings."""
        if filename is None:
            filename = os.path.basename(file_path)

        for encoding in self.SUPPORTED_ENCODINGS:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    return f.read()
            except UnicodeDecodeError:
                continue
            except Exception as e:
                self.logger.error(f"Error reading file {filename}: {e}")
                return None

        self.logger.error(f"Cannot read file {filename} with any supported encoding")
        return None
    
    def extract_date_from_path(self, file_path: str) -> str:
        """Extract date in format YYYY.MM.DD from file path."""
        match = re.search(r'(\d{4}\.\d{2}\.\d{2})', file_path)
        return match.group(1) if match else ''
    
    @staticmethod
    def extract_revision_from_filename(
        filename: str, left_delimiter: str = '', right_delimiter: str = ''
    ) -> str:
        """Extract revision number from *filename* using left/right delimiters."""
        if not left_delimiter or not right_delimiter:
            return ''
        left_pos = filename.rfind(left_delimiter)
        if left_pos == -1:
            return ''
        start_pos = left_pos + len(left_delimiter)
        right_pos = filename.find(right_delimiter, start_pos)
        if right_pos == -1:
            return ''
        return filename[start_pos:right_pos].strip()

 # -- revision status and indexing helpers -------------------------

    @staticmethod
    def _parse_revision(revision_str: str) -> tuple[str, str, str]:
        """Parse revision into (full_str, numeric_part, alpha_part).
        
        Examples:
            'A'    → ('A', '', 'A')
            '2'    → ('2', '2', '')
            '2A'   → ('2A', '2', 'A')
            'A01'  → ('A01', '01', 'A')
        """
        if not revision_str:
            return ('', '', '')
        
        numeric = ''
        alpha = ''
        
        # Extract digits and letters (in order of appearance)
        for char in revision_str:
            if char.isdigit():
                numeric += char
            elif char.isalpha():
                alpha += char
        
        return (revision_str, numeric, alpha)

    @staticmethod
    def _compare_revisions(rev1: str, rev2: str) -> int:
        """Compare two revisions. Return:
        -1 if rev1 < rev2 (rev1 is older)
        +1 if rev1 > rev2 (rev1 is newer)
        0 if equal
        
        Sorting rules:
        - Pure alpha < Pure numeric < Mixed
        - Within category, sort by (numeric_part, then alpha_part)
        """
        if rev1 == rev2:
            return 0
        
        full1, num1, alpha1 = PCFProcessor._parse_revision(rev1)
        full2, num2, alpha2 = PCFProcessor._parse_revision(rev2)
        
        # Determine types
        type1 = 'none' if not full1 else ('alpha' if not num1 else ('numeric' if not alpha1 else 'mixed'))
        type2 = 'none' if not full2 else ('alpha' if not num2 else ('numeric' if not alpha2 else 'mixed'))
        
        type_order = {'none': -1, 'alpha': 0, 'numeric': 1, 'mixed': 2}
        
        # First compare by type
        if type_order[type1] != type_order[type2]:
            return -1 if type_order[type1] < type_order[type2] else 1
        
        # Within same type, compare by numeric then alpha
        if type1 == 'none':
            return 0
        
        if type1 == 'alpha':
            # Pure alpha: A < B < C
            return -1 if alpha1 < alpha2 else (1 if alpha1 > alpha2 else 0)
        
        if type1 == 'numeric':
            # Pure numeric: compare as integers
            try:
                n1 = int(num1)
                n2 = int(num2)
                return -1 if n1 < n2 else (1 if n1 > n2 else 0)
            except ValueError:
                return -1 if num1 < num2 else (1 if num1 > num2 else 0)
        
        # Mixed: first compare numeric part, then alpha part
        try:
            n1 = int(num1) if num1 else 0
            n2 = int(num2) if num2 else 0
        except ValueError:
            n1 = num1
            n2 = num2
        
        if n1 != n2:
            return -1 if n1 < n2 else 1
        
        # Numeric parts equal, compare alpha
        return -1 if alpha1 < alpha2 else (1 if alpha1 > alpha2 else 0)

    @staticmethod
    def _revision_sort_key(item: dict) -> tuple:
        """Generate sort key for revision + date.
        
        Used for sorting records by (LINEID, Date, Revision) in correct order.
        """
        # Parse revision
        full, num, alpha = PCFProcessor._parse_revision(item.get('Revision', ''))
        
        type_order = {'none': -1, 'alpha': 0, 'numeric': 1, 'mixed': 2}
        rev_type = 'none' if not full else ('alpha' if not num else ('numeric' if not alpha else 'mixed'))
        
        # Numeric part as integer if possible
        try:
            num_int = int(num) if num else 0
        except ValueError:
            num_int = float('inf')
        
        # Return tuple for multi-level sort
        return (
            type_order[rev_type],  # Type priority
            num_int,               # Numeric component (as int)
            num,                   # Numeric component (as string, fallback)
            alpha,                 # Alpha component
            item.get('Date', ''),  # Date as fallback
        )

    def _enrich_lengths_with_status_and_index(
        self, table3: list[dict]
    ) -> list[dict]:
        """Enrich pipe length records with Status and Index columns.

        Two-level grouping:
        1. Group by LINEID (outer level)
        2. Within each LINEID, group by (Date, Revision, Folder_Path)

        Each LINEID gets its own Index sequence (1, 2, 3...).
        Each LINEID gets its own 'Actual' status for latest group.

        Returns enriched table3 with new columns.
        """
        if not table3:
            return table3

        # Step 1: Group by LINEID
        lineid_groups: defaultdict[str, list[dict]] = defaultdict(list)     # ← defaultdict
        for record in table3:
            lineid_groups[record.get('LINEID', 'UNKNOWN')].append(record)

        enriched: list[dict] = []
        total_sub_groups_count = 0

        # Step 2: Process each LINEID separately
        for lineid, lineid_records in lineid_groups.items():

            # Step 3: Sub-group by (Date, Revision, Folder_Path)
            sub_groups: defaultdict[tuple, list[dict]] = defaultdict(list)   # ← defaultdict
            for record in lineid_records:
                key = (
                    record.get('Date', ''),
                    record.get('Revision', ''),
                    record.get('Folder Path', ''),
                )
                sub_groups[key].append(record)

            # Step 4: Sort by (Date, Revision)
            sorted_subs = sorted(
                sub_groups.items(),
                key=lambda item: (item[0][0], item[0][1]),
            )
            num_subs = len(sorted_subs)

            # Step 5: Assign Index and Status
            for sub_idx, (_key, records) in enumerate(sorted_subs, start=1):
                status = 'Actual' if sub_idx == num_subs else 'Obsolete'    # ← вычисляется один раз на группу
                for record in records:
                    record['Index'] = sub_idx
                    record['Status'] = status
                    enriched.append(record)

            total_sub_groups_count += num_subs

        self.logger.info(
            f"Enriched {len(enriched)} length records with Status and Index. "
            f"Processed {len(lineid_groups)} LINEIDs with "
            f"{total_sub_groups_count} total unique combinations."
        )
        return enriched

    @staticmethod
    def _find_pipeline_ref_lines(lines: list[str]) -> list[tuple[int, str]]:
        """Find all non-indented PIPELINE-REFERENCE lines.

        Returns list of (line_index, stripped_line_text).
        """
        return [
            (idx, line.strip())
            for idx, line in enumerate(lines)
            if line and not line[0].isspace() and line.strip().startswith('PIPELINE-REFERENCE')
        ]


    def add_error(
        self, pcf_name: str, pipeline_ref: str, error_type: str,
        full_path: str | None = None,
    ) -> None:
        """Add error to error log - one row per PIPELINE-REFERENCE."""
        self.error_log.append({
            'PCF_File': pcf_name,
            'PIPELINE-REFERENCE': pipeline_ref,
            'Error_Type': error_type,
            'Full_Path': full_path or '',
            'Date': self.extract_date_from_path(full_path) if full_path else '',
            'Revision': self._get_revision(pcf_name) if full_path else '',
        })
    
    @staticmethod                                          # ← NEW
    def extract_pipeline_refs_from_filename(filename):     # ← убран self
        """Extract potential PIPELINE-REFERENCE from filename."""
        name = os.path.splitext(filename)[0]
        name = re.sub(r'-Rev\.\d+$', '', name)
        return name
    
    def validate_pcf_file(self, file_path, filename):
        """Validate PCF file for errors."""
        content = self._read_pcf_file(file_path, filename)

        if content is None:
            self.add_error(filename, "N/A", "Read Error", full_path=file_path)
            return None

        if not content.strip():
            self.add_error(filename, "N/A", "Empty", full_path=file_path)
            return None

        lines = content.split('\n')

        # ← NEW: вычисляем ref_values ОДИН раз (было: split вызывался 6 раз)
        ref_values: list[str] = []
        for _, text in self._find_pipeline_ref_lines(lines):
            parts = text.split(None, 1)
            ref_values.append(parts[1] if len(parts) > 1 else 'UNKNOWN')

        # Check: multiple PIPELINE-REFERENCE in one file
        if len(ref_values) > 1:
            for ref_value in ref_values:
                self.add_error(filename, ref_value, "Multiple PIPELINE-REFERENCE", full_path=file_path)

        expected_from_filename = self.extract_pipeline_refs_from_filename(filename)

        # ← NEW: один цикл вместо двух (duplication + filename match)
        for ref_value in ref_values:
            # Duplication check
            if ref_value in self.seen_pipeline_refs:
                self.add_error(
                    filename, ref_value,
                    "Duplication of PIPELINE-REFERENCE", full_path=file_path,
                )
                first_info = self.seen_pipeline_refs[ref_value]
                already_logged = any(
                    err['PCF_File'] == first_info['filename']
                    and err['PIPELINE-REFERENCE'] == ref_value
                    and err['Error_Type'] == "Duplication of PIPELINE-REFERENCE"
                    for err in self.error_log
                )
                if not already_logged:
                    self.add_error(
                        first_info['filename'], ref_value,
                        "Duplication of PIPELINE-REFERENCE",
                        full_path=first_info['full_path'],
                    )
            else:
                self.seen_pipeline_refs[ref_value] = {
                    'filename': filename,
                    'full_path': file_path,
                }

            # Filename match check
            if (ref_value not in expected_from_filename
                    and expected_from_filename not in ref_value):
                self.add_error(
                    filename, ref_value,
                    "PIPELINE-REFERENCE doesn't match PCF name",
                    full_path=file_path,
                )

        return content
    
    def parse_pcf_file(
        self, file_path: str, content: str | None = None,          # ← NEW: optional content
    ) -> tuple[dict | None, list[InstrumentData] | None]:
        """Parse PCF file for pipeline attributes and instruments with coordinates."""
        if content is None:                                         # ← NEW: read only if not provided
            content = self._read_pcf_file(file_path)
        if content is None:
            return None, None

        pipeline_data: dict[str, str] = {}
        instruments: list[InstrumentData] = []
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
                if any(line.startswith(bt) for bt in self.COMPONENT_BLOCK_TYPES):
                    break
                parts = line.split(None, 1)
                if len(parts) == 2:
                    pipeline_data[parts[0]] = parts[1]

        # Extract INSTRUMENT blocks with coordinates
        instrument_pattern = r'INSTRUMENT\s*\n(.*?)(?=\n[A-Z-]+(?:\s|\n)|\Z)'
        for block in re.finditer(instrument_pattern, content, re.DOTALL):
            block_content = block.group(1)
            tag_match = re.search(r'TAG\s+(.+)', block_content, re.MULTILINE)
            if not tag_match:
                continue
            tag = tag_match.group(1).strip()
            if not tag:
                continue
        
            x, y, z = '', '', ''
            coord_match = re.search(
                r'CENTRE-POINT\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)', block_content
            ) or re.search(
                r'END-POINT\s+([\d.-]+)\s+([\d.-]+)\s+([\d.-]+)', block_content
            )
            if coord_match:
                x, y, z = coord_match.group(1), coord_match.group(2), coord_match.group(3)

            instruments.append(InstrumentData(tag=tag, x=x, y=y, z=z))

        return pipeline_data, instruments
    
    # -- length parsing helpers -----------------------------------------

    @staticmethod
    def _parse_endpoint(parts: list[str]) -> Endpoint | None:
        """Try to build an Endpoint from split line tokens."""
        if len(parts) < 5:
            return None
        try:
            x, y, z = float(parts[1]), float(parts[2]), float(parts[3])
            size_str = parts[4].replace('.', '').replace('-', '')
            size = float(parts[4]) if size_str.isdigit() else None
            return Endpoint(x=x, y=y, z=z, size=size)
        except (ValueError, IndexError):
            return None

    @staticmethod
    def _compute_segment_length(ep1: Endpoint, ep2: Endpoint, seg_type: str) -> float:
        """Compute segment length depending on component type."""
        dx = ep2.x - ep1.x
        dy = ep2.y - ep1.y
        dz = ep2.z - ep1.z
        if seg_type in ('ELBOW', 'BEND'):
            return (abs(dx) + abs(dy) + abs(dz)) / 4 * math.pi
        if seg_type == 'TEE':
            base = math.sqrt(dx ** 2 + dy ** 2 + dz ** 2)
            return base * 1.5
        return math.sqrt(dx ** 2 + dy ** 2 + dz ** 2)

    def parse_pcf_for_lengths(
        self, file_path: str, include_continuation: bool = False,
        content: str | None = None,                                 # ← NEW: optional content
    ) -> list[dict]:
        """Parse PCF file to calculate pipe lengths and count components."""
        if content is None:                                         # ← NEW: read only if not provided
            content = self._read_pcf_file(file_path)
        if content is None:
            return []
        
        lines = content.split('\n')

        # Global UNITS-BORE (first N lines)                    ← комментарий обновлён
        global_unit: str | None = None
        for line in lines[:self._HEADER_SCAN_LINES]:           # ← было [:20]
            stripped = line.strip()
            if stripped.startswith('UNITS-BORE'):
                parts = stripped.split()
                if len(parts) > 1:
                    global_unit = parts[1]
                break

        # Locate PIPELINE-REFERENCE blocks
        # ← NEW: используем общий хелпер вместо дублированного цикла
        ref_lines = self._find_pipeline_ref_lines(lines)
        pipeline_blocks: list[dict] = []
        for idx, text in ref_lines:
            if pipeline_blocks:
                pipeline_blocks[-1]['end'] = idx
            parts = text.split(None, 1)
            pipeline_blocks.append({
                'lineid': parts[1] if len(parts) > 1 else 'UNKNOWN',
                'start': idx,
                'end': None,
            })
        if pipeline_blocks and pipeline_blocks[-1]['end'] is None:
            pipeline_blocks[-1]['end'] = len(lines)

        output: list[dict] = []

        for block in pipeline_blocks:
            lineid = block['lineid']
            block_start, block_end = block['start'], block['end']

            # Per-block UNITS-BORE override
            unit = global_unit
            for line in lines[block_start:min(block_start + self._UNIT_SCAN_RANGE, block_end)]:  # ← было 30
                stripped = line.strip()
                if stripped.startswith('UNITS-BORE'):
                    parts = stripped.split()
                    if len(parts) > 1:
                        unit = parts[1]
                    break
            if not unit:
                self.logger.warning(f"UNIT not found for PIPELINE {lineid}")
                unit = self.DEFAULT_UNIT

            # Find component segments inside block
            segments: list[dict] = []
            for idx in range(block_start + 1, block_end):
                line = lines[idx]
                if line and not line[0].isspace() and line.strip() and not line.strip().startswith('PIPELINE-REFERENCE'):
                    segments.append({'type': line.strip(), 'start_idx': idx})

            results: list[PipeSegmentResult] = []
            for i, seg in enumerate(segments):
                start = seg['start_idx']
                end = segments[i + 1]['start_idx'] if i + 1 < len(segments) else block_end
                seg_lines = lines[start:end]
                seg_type = seg['type']

                if not include_continuation and any('CONTINUATION' in ln for ln in seg_lines):
                    continue

                # Collect endpoints
                endpoints: list[Endpoint] = []
                for ln in seg_lines:
                    stripped = ln.strip()
                    if stripped.startswith('END-POINT') or stripped.startswith('CO-ORDS'):
                        ep = self._parse_endpoint(stripped.split())
                        if ep is not None:
                            endpoints.append(ep)

                if len(endpoints) >= 2:
                    ep1, ep2 = endpoints[0], endpoints[1]
                    size1 = ep1.size if ep1.size is not None else 0.0
                    size2 = ep2.size if ep2.size is not None else 0.0
                    final_size = max(size1, size2)
                    if final_size == 0:
                        continue
                    length = self._compute_segment_length(ep1, ep2, seg_type)
                    results.append(PipeSegmentResult(
                        seg_type=seg_type, size=final_size, length=length,
                        is_support=(seg_type == 'SUPPORT'),
                        is_flange=(seg_type == 'FLANGE'),
                        is_valve=(seg_type == 'VALVE'),
                    ))
                elif len(endpoints) == 1 and seg_type == 'SUPPORT' and endpoints[0].size:
                    results.append(PipeSegmentResult(
                        seg_type=seg_type, size=endpoints[0].size, length=0,
                        is_support=True,
                    ))

            if not results:
                continue
            
            # Aggregate by size
            df = pd.DataFrame([{
                'size': r.size, 'length': r.length,
                'is_support': int(r.is_support),
                'is_flange': int(r.is_flange),
                'is_valve': int(r.is_valve),
            } for r in results])
            grouped = df.groupby('size').agg(
                length=('length', 'sum'),
                is_support=('is_support', 'sum'),
                is_flange=('is_flange', 'sum'),
                is_valve=('is_valve', 'sum'),
            ).reset_index()

            for _, row in grouped.iterrows():
                output.append({
                    'LINEID': lineid,
                    'SIZE': row['size'],
                    'UNIT': unit,
                    'LENGTH': math.ceil(row['length']),
                    'SUPPORT_QTY': int(row['is_support']),
                    'FLANGE_QTY': int(row['is_flange']),
                    'VALVE_QTY': int(row['is_valve']),
                })
        
        return output

    
    # -- Excel saving ---------------------------------------------------

    def save_excel(self, df: pd.DataFrame, filepath: str, sheetname: str) -> None:
        """Save DataFrame to Excel with auto-column-width.

        Raises PermissionError / Exception on failure (caller handles UI).
        """
        if df.empty:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                pd.DataFrame({'Status': ['No data']}).to_excel(
                    writer, index=False, sheet_name=sheetname,
                )
        else:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name=sheetname)
                ws = writer.sheets[sheetname]
                for col in ws.columns:
                    max_len = max(
                        (len(str(cell.value)) for cell in col if cell.value is not None),
                        default=0,
                    )
                    ws.column_dimensions[col[0].column_letter].width = min(max_len + 2, 50)
        self.logger.info(f"Saved: {os.path.basename(filepath)}")

    # -- folder scanning ------------------------------------------------

    @staticmethod
    def find_pcf_files(folder_path: str) -> list[str]:
        """Recursively find all .pcf files under *folder_path*."""
        pcf_files: list[str] = []
        for root_dir, _dirs, files in os.walk(folder_path):
            for fname in files:
                if fname.lower().endswith('.pcf'):
                    pcf_files.append(os.path.join(root_dir, fname))
        return pcf_files

    # -- sub-steps of processing ----------------------------------------

    def _validate_files(
        self, pcf_files: list[str], progress_callback=None,
    ) -> None:
        """Run validation pass on all files."""
        total = len(pcf_files)
        self.logger.info("Validation pass - checking for errors...")
        for i, fp in enumerate(pcf_files):
            if progress_callback:
                progress_callback(i, total * 3,
                                  f"Validation {i+1}/{total}: {os.path.basename(fp)}")
            self.validate_pcf_file(fp, os.path.basename(fp))

    def _collect_pipeline_keys(
        self, pcf_files: list[str], progress_callback=None,
    ) -> set[str]:
        """First pass: determine the union of all pipeline attribute keys."""
        total = len(pcf_files)
        keys: set[str] = set()
        self.logger.info("First pass - analyzing file structure...")
        for i, fp in enumerate(pcf_files):
            if progress_callback:
                progress_callback(total + i, total * 3,
                                  f"Analysis {i+1}/{total}: {os.path.basename(fp)}")
            data, _ = self.parse_pcf_file(fp)
            if data:
                keys.update(data.keys())
        self.logger.info(f"Found {len(keys)} unique parameters across all files")
        return keys

    def _build_file_metadata(self, file_path: str) -> dict:
        """Common metadata columns for every row."""
        filename = os.path.basename(file_path)
        return {
            'File_Name': filename,
            'Full_Path': file_path,
            'Date': self.extract_date_from_path(file_path),
            'Revision': self._get_revision(filename),
        }

    def _collect_data(
        self, pcf_files: list[str], all_keys: set[str],
        generate_reports: dict[str, bool], include_continuation: bool,
        progress_callback=None,
    ) -> tuple[list[dict], list[dict], list[dict], int, int]:
        """Second pass: collect rows for all selected reports."""
        total = len(pcf_files)
        table1: list[dict] = []
        table2: list[dict] = []
        table3: list[dict] = []
        processed = errors = 0

        self.logger.info("Second pass - collecting data...")
        for i, fp in enumerate(pcf_files):
            if progress_callback:
                progress_callback(total * 2 + i, total * 3,
                                  f"Processing {i+1}/{total}: {os.path.basename(fp)}")

            filename = os.path.basename(fp)
            meta = self._build_file_metadata(fp)

            # ← NEW: читаем файл ОДИН раз, передаём содержимое обоим парсерам
            content = self._read_pcf_file(fp)

            pipeline_data, instruments = self.parse_pcf_file(fp, content=content)

            if pipeline_data is not None:
                if generate_reports.get('pipeline'):
                    row = dict(meta)
                    for key in sorted(all_keys):
                        row[key] = pipeline_data.get(key, '')
                    table1.append(row)

                if generate_reports.get('instruments') and instruments:
                    for inst in instruments:
                        table2.append({
                            **meta,
                            'INSTRUMENT': inst.tag,
                            'X': inst.x, 'Y': inst.y, 'Z': inst.z,
                        })
                processed += 1
            else:
                errors += 1
                self.logger.warning(f"Cannot process file: {fp}")

            # ← NEW: передаём content, не перечитываем файл
            if generate_reports.get('lengths') and content is not None:
                for rec in self.parse_pcf_for_lengths(fp, include_continuation, content=content):
                    table3.append({
                        'Name': filename,
                        'LINEID': rec['LINEID'],
                        'SIZE': rec['SIZE'],
                        'UNIT': rec['UNIT'],
                        'LENGTH': rec['LENGTH'],
                        'SUPPORT QTY': rec['SUPPORT_QTY'],
                        'FLANGE QTY': rec['FLANGE_QTY'],
                        'VALVE QTY': rec['VALVE_QTY'],
                        'Folder Path': os.path.dirname(fp),
                        'Date': meta['Date'],
                        'Revision': meta['Revision'],
                    })

        return table1, table2, table3, processed, errors

    def _reorder_columns_for_lengths(self, df: pd.DataFrame) -> pd.DataFrame:
        """Reorder columns in lengths report for better readability.
        
        Preferred order:
        LINEID, Index, Status, SIZE, UNIT, LENGTH, QTYs, Date, Revision, Name, Path
        """
        if df.empty:
            return df
        
        # Define preferred order
        preferred_order = [
            'LINEID', 'Index', 'Status', 'SIZE', 'UNIT', 'LENGTH',
            'SUPPORT QTY', 'FLANGE QTY', 'VALVE QTY',
            'Date', 'Revision', 'Name', 'Folder Path',
        ]
        
        # Keep only columns that exist in the dataframe
        existing_cols = [col for col in preferred_order if col in df.columns]
        
        # Add any remaining columns (in case of new fields in future)
        remaining_cols = [col for col in df.columns if col not in existing_cols]
        
        # Reorder
        final_order = existing_cols + remaining_cols
        return df[final_order]


    def _save_reports(
        self, tables: dict[str, list[dict]],
        save_paths: dict[str, str],
        generate_reports: dict[str, bool],
    ) -> list[str]:
        """Save selected DataFrames to Excel files. Returns list of created basenames."""
        report_map = {
            'pipeline': ('Pipeline_Attributes', tables.get('pipeline', [])),
            'instruments': ('Instruments', tables.get('instruments', [])),
            'lengths': ('PipeLengthFromPCF', tables.get('lengths', [])),
            'errors': ('Errors', tables.get('errors', [])),
        }
        created: list[str] = []
        for key, (sheet, data) in report_map.items():
            if not generate_reports.get(key):
                continue
            path = save_paths.get(key)
            if not path:
                continue
            df = pd.DataFrame(data) if data else pd.DataFrame()
            
            # ✨ NEW: Reorder columns for lengths report
            if key == 'lengths' and not df.empty:
                df = self._reorder_columns_for_lengths(df)
            
            self.save_excel(df, path, sheet)
            created.append(os.path.basename(path))
        return created

    @staticmethod
    def _build_summary(result: ProcessingResult) -> str:
        """Format a human-readable summary string."""
        files_list = '\n'.join(f'  - {f}' for f in result.created_files)
        return (
            f"Processing completed!\n\n"
            f"Statistics:\n"
            f"  Processed files: {result.processed_files}\n"
            f"  Errors: {result.errors}\n"
            f"  Instruments found: {result.instruments_count}\n"
            f"  Pipe length records: {result.length_records}\n"
            f"  Pipeline parameters found: {result.pipeline_keys_count}\n"
            f"  Validation errors found: {result.validation_errors_count}\n\n"
            f"Files created:\n{files_list}"
        )

    # -- main entry point (no messagebox) -------------------------------

    def process_folder(
        self,
        folder_path: str,
        progress_callback=None,
        save_paths: dict[str, str] | None = None,
        include_continuation: bool = False,
        generate_reports: dict[str, bool] | None = None,
        revision_settings: dict | None = None,
    ) -> ProcessingResult:
        """Process all PCF files and return a pure-data result (no UI calls)."""
        if generate_reports is None:
            generate_reports = {
                'pipeline': True, 'instruments': True,
                'lengths': True, 'errors': True,
            }
        if save_paths is None:
            save_paths = {}

        # Apply revision settings
        if revision_settings:
            self.revision = RevisionSettings(
                enabled=revision_settings.get('enabled', False),
                left_delimiter=revision_settings.get('left_delimiter', ''),
                right_delimiter=revision_settings.get('right_delimiter', ''),
            )

        self.error_log = []
        self.seen_pipeline_refs = {}

        pcf_files = self.find_pcf_files(folder_path)
        if not pcf_files:
            return ProcessingResult(success=False, error_message="No PCF files found in selected folder!")

        self.logger.info(f"Found {len(pcf_files)} PCF files to process")
        self.logger.info(f"Include CONTINUATION: {'YES' if include_continuation else 'NO'}")
        self.logger.info(f"Generate reports: {generate_reports}")

        # 1. Validate
        self._validate_files(pcf_files, progress_callback)

        # 2. Collect keys
        all_keys = self._collect_pipeline_keys(pcf_files, progress_callback)

        # 3. Collect data
        t1, t2, t3, processed, errs = self._collect_data(
            pcf_files, all_keys, generate_reports, include_continuation, progress_callback,
        )

        # 3.5. ✨ NEW: Enrich pipe length records with Status and Index
        if generate_reports.get('lengths') and t3:
            t3 = self._enrich_lengths_with_status_and_index(t3)


        # 4. Save reports
        tables = {
            'pipeline': t1, 'instruments': t2,
            'lengths': t3, 'errors': self.error_log,
        }
        created = self._save_reports(tables, save_paths, generate_reports)

        self.logger.info(f"Processing completed. Processed={processed}, Errors={errs}")

        result = ProcessingResult(
            success=True,
            processed_files=processed,
            errors=errs,
            instruments_count=len(t2),
            length_records=len(t3),
            pipeline_keys_count=len(all_keys),
            validation_errors_count=len(self.error_log),
            created_files=created,
        )
        return result


# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------

def _center_window(window: tk.Tk | tk.Toplevel, w: int, h: int) -> None:
    """Center a Tk/Toplevel window on screen."""
    window.update_idletasks()
    x = (window.winfo_screenwidth() - w) // 2
    y = (window.winfo_screenheight() - h) // 2
    window.geometry(f"{w}x{h}+{x}+{y}")


# ---------------------------------------------------------------------------
# UI widgets
# ---------------------------------------------------------------------------

class ProgressWindow:
    """Modal progress window."""

    def __init__(self, parent: tk.Tk) -> None:
        self.window = tk.Toplevel(parent)
        self.window.title("Processing files...")
        self.window.geometry("500x180")
        self.window.resizable(False, False)
        self.window.transient(parent)
        self.window.grab_set()
        _center_window(self.window, 500, 180)              # ← было self._center(500, 180)

        self.label = tk.Label(self.window, text="Preparing...", pady=10)
        self.label.pack()
        self.progress = ttk.Progressbar(self.window, mode='determinate', length=450)
        self.progress.pack(pady=10)
        self.status_label = tk.Label(self.window, text="", wraplength=450)
        self.status_label.pack()

    def update_progress(self, current: int, total: int, status: str) -> None:
        if total > 0:
            pct = current / total * 100
            self.progress['value'] = pct
            self.label.config(text=f"Progress: {current}/{total} ({pct:.1f}%)")
            self.status_label.config(text=status)
            self.window.update()

    def close(self) -> None:
        self.window.destroy()


class RevisionDefinitionDialog:
    """Dialog for defining revision extraction pattern."""

    def __init__(self, parent: tk.Tk, folder_path: str) -> None:
        self.result: dict | None = None
        self.folder_path = folder_path
        self.sample_filename: str | None = None

        self.window = tk.Toplevel(parent)
        self.window.title("Define Revision Pattern")
        self.window.geometry("700x550")
        self.window.resizable(False, False)
        self.window.transient(parent)
        self.window.grab_set()
        _center_window(self.window, 700, 550)               # ← было self._center(700, 550)
        self._build_ui()

    # -- layout ---------------------------------------------------------

    def _build_ui(self) -> None:
        instruction_text = (
            "Define how to extract revision number from PCF filenames.\n"
            "Select a sample file and specify left and right delimiters.\n\n"
            "Examples:\n"
            "  File: '082755C-057-ISO-P257A02-HO57048-0-01-0_0.PCF'\n"
            "  Left: '-0_'  Right: '.PCF'  -> Revision: '0'\n\n"
            "  File: '886-A-62004-010 - Rev.02.pcf'\n"
            "  Left: 'Rev.'  Right: '.pcf'  -> Revision: '02'"
        )
        tk.Label(self.window, text=instruction_text, justify=tk.LEFT,
                 padx=10, pady=10, wraplength=660).pack(anchor=tk.W)

        # Sample file
        file_frame = tk.LabelFrame(self.window, text="Step 1: Select Sample File", padx=10, pady=10)
        file_frame.pack(padx=20, pady=(5, 10), fill=tk.X)
        self.file_label = tk.Label(file_frame, text="No file selected", fg="gray", wraplength=500)
        self.file_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        tk.Button(file_frame, text="Browse...", command=self._select_sample, padx=10).pack(side=tk.RIGHT)

        # Delimiters
        delim_frame = tk.LabelFrame(self.window, text="Step 2: Define Delimiters", padx=10, pady=10)
        delim_frame.pack(padx=20, pady=(5, 10), fill=tk.X)
        tk.Label(delim_frame, text="Left Delimiter:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.left_entry = tk.Entry(delim_frame, width=30)
        self.left_entry.grid(row=0, column=1, padx=10, pady=5)
        tk.Label(delim_frame, text="Right Delimiter:").grid(row=1, column=0, sticky=tk.W, pady=5)
        self.right_entry = tk.Entry(delim_frame, width=30)
        self.right_entry.grid(row=1, column=1, padx=10, pady=5)
        
        # Preview
        preview_frame = tk.LabelFrame(self.window, text="Preview", padx=10, pady=10)
        preview_frame.pack(padx=20, pady=(5, 15), fill=tk.BOTH, expand=True)
        self.preview_label = tk.Label(preview_frame, text="Enter delimiters to see preview", 
                                      fg="gray", justify=tk.LEFT, wraplength=640)
        self.preview_label.pack(fill=tk.BOTH, expand=True)

        self.left_entry.bind('<KeyRelease>', self._update_preview)
        self.right_entry.bind('<KeyRelease>', self._update_preview)

        # Buttons
        btn_frame = tk.Frame(self.window)
        btn_frame.pack(side=tk.BOTTOM, pady=15)
        tk.Button(btn_frame, text="Apply", command=self._apply,
                  bg="#4CAF50", fg="white", padx=30, pady=8,
                  font=("Arial", 10, "bold")).pack(side=tk.LEFT, padx=10)
        tk.Button(btn_frame, text="Cancel", command=self._cancel,
                  padx=30, pady=8, font=("Arial", 10)).pack(side=tk.LEFT, padx=10)

    # -- callbacks ------------------------------------------------------

    def _select_sample(self) -> None:
        path = filedialog.askopenfilename(
            title="Select sample PCF file", initialdir=self.folder_path,
            filetypes=[("PCF files", "*.pcf"), ("PCF files", "*.PCF"), ("All files", "*.*")],
        )
        if path:
            self.sample_filename = os.path.basename(path)
            self.file_label.config(text=self.sample_filename, fg="black")
            self._update_preview()

    def _update_preview(self, _event=None) -> None:
        if not self.sample_filename:
            self.preview_label.config(text="Please select a sample file first", fg="gray")
            return
        left, right = self.left_entry.get(), self.right_entry.get()
        if not left or not right:
            self.preview_label.config(
                text=f"Filename: {self.sample_filename}\n\nEnter both delimiters to see result", fg="gray")
            return

        revision = PCFProcessor.extract_revision_from_filename(self.sample_filename, left, right)
        if revision:
            self.preview_label.config(
                text=f"Filename: {self.sample_filename}\n\n"
                     f"Extracted Revision: '{revision}'\n\nPattern looks good!",
                fg="green", font=("Arial", 10))
        else:
            self.preview_label.config(
                text=f"Filename: {self.sample_filename}\n\n"
                     f"Could not extract revision with given delimiters", fg="red")

    def _apply(self) -> None:
        left, right = self.left_entry.get(), self.right_entry.get()
        if not left or not right:
            messagebox.showwarning("Warning", "Please enter both delimiters!")
            return
        if not self.sample_filename:
            messagebox.showwarning("Warning", "Please select a sample file!")
            return
        self.result = {'enabled': True, 'left_delimiter': left, 'right_delimiter': right}
        self.window.destroy()

    def _cancel(self) -> None:
        self.result = None
        self.window.destroy()


# ---------------------------------------------------------------------------
# Main application (UI class)
# ---------------------------------------------------------------------------

class PCFProcessorApp:
    """Tkinter application that drives :class:`PCFProcessor`."""

    VERSION = "7.2"
    _REPORT_CONFIGS = [
        ('pipeline',     'Pipeline Attributes',          'PCF_Pipeline_Attributes'),
        ('instruments',  'Instruments',                  'PCF_Instruments'),
        ('lengths',      'PipeLengthFromPCF',            'PCF_PipeLengthFromPCF'),
        ('errors',       'PCF_Errors (Validation Log)',  'PCF_Errors'),
    ]

    def __init__(self) -> None:
        self.processor = PCFProcessor()
        self.selected_folder: str | None = None
        self.revision_settings: dict = {'enabled': False, 'left_delimiter': '', 'right_delimiter': ''}

        self.root = tk.Tk()
        self.root.title(f"PCF File Processor v{self.VERSION}")
        self.root.geometry("700x650")
        self.root.resizable(False, False)
        _center_window(self.root, 700, 650)                 # ← было self._center(700, 650)

        self._report_vars: dict[str, tk.BooleanVar] = {}
        self._build_ui()

    # -- helpers --------------------------------------------------------

    @staticmethod
    def _open_folder(path: str) -> None:
        """Cross-platform folder open."""
        if sys.platform == 'win32':
            os.startfile(path)
        elif sys.platform == 'darwin':
            subprocess.Popen(['open', path])
        else:
            subprocess.Popen(['xdg-open', path])

    # -- UI building ----------------------------------------------------

    def _build_ui(self) -> None:
        tk.Label(self.root, text=f"PCF File Processor v{self.VERSION}",
                 font=("Arial", 16, "bold")).pack(pady=20)
        self._build_folder_section()
        self._build_reports_section()
        self._build_revision_section()
        self._build_options_section()
        self._build_start_button()
        tk.Label(self.root,
                 text="Supported formats: .pcf  |  Recursive folder search  |  Automatic error detection",
                 font=("Arial", 8), fg="gray").pack(side="bottom", pady=10)

    def _build_folder_section(self) -> None:
        frame = tk.LabelFrame(self.root, text="Step 1: Select PCF Folder",
                              font=("Arial", 10, "bold"), padx=10, pady=10)
        frame.pack(padx=20, pady=10, fill=tk.X)
        frame.pack_propagate(False)
        frame.configure(height=70)
        # Pack button FIRST so it keeps its space; label fills the rest
        tk.Button(frame, text="Browse...", command=self._select_folder,
                  padx=10).pack(side=tk.RIGHT, padx=(10, 0))
        self._folder_label = tk.Label(frame, text="No folder selected",
                                      fg="gray", wraplength=520, anchor="w", justify=tk.LEFT)
        self._folder_label.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def _build_reports_section(self) -> None:
        frame = tk.LabelFrame(self.root, text="Step 2: Select Reports to Generate",
                              font=("Arial", 10, "bold"), padx=10, pady=10)
        frame.pack(padx=20, pady=10, fill=tk.X)
        for key, label, _ in self._REPORT_CONFIGS:
            var = tk.BooleanVar(value=True)
            self._report_vars[key] = var
            tk.Checkbutton(frame, text=label, variable=var, font=("Arial", 10)).pack(anchor=tk.W, pady=5)

    def _build_revision_section(self) -> None:
        frame = tk.LabelFrame(self.root, text="Step 3: Define Revision (Optional)",
                              font=("Arial", 10, "bold"), padx=10, pady=10)
        frame.pack(padx=20, pady=10, fill=tk.X)
        self._revision_status = tk.Label(frame, text="Revision extraction: Disabled",
                                         fg="gray", font=("Arial", 9))
        self._revision_status.pack(side=tk.LEFT, padx=10)
        tk.Button(frame, text="Define Revision Pattern",
                  command=self._define_revision, padx=10, pady=5).pack(side=tk.RIGHT, padx=5)

    def _build_options_section(self) -> None:
        frame = tk.LabelFrame(self.root, text="Step 4: Options",
                              font=("Arial", 10, "bold"), padx=10, pady=10)
        frame.pack(padx=20, pady=10, fill=tk.X)
        self._var_continuation = tk.BooleanVar(value=False)
        tk.Checkbutton(frame, text="Include blocks with CONTINUATION (default: excluded)",
                       variable=self._var_continuation, font=("Arial", 10)).pack(anchor=tk.W, pady=5)

    def _build_start_button(self) -> None:
        tk.Button(self.root, text="START / GENERATE", command=self._start_processing,
                  font=("Arial", 13, "bold"), bg="#4CAF50", fg="white",
                  padx=30, pady=15).pack(pady=20)

    # -- callbacks ------------------------------------------------------

    def _select_folder(self) -> None:
        folder = filedialog.askdirectory(title="Select folder with PCF files")
        if folder:
            self.selected_folder = folder
            self._folder_label.config(text=folder, fg="black")

    def _define_revision(self) -> None:
        if not self.selected_folder:
            messagebox.showwarning("Warning", "Please select a PCF folder first!")
            return
        dialog = RevisionDefinitionDialog(self.root, self.selected_folder)
        self.root.wait_window(dialog.window)
        if dialog.result:
            self.revision_settings.update(dialog.result)
            self._revision_status.config(
                text=(f"Revision: Enabled  "
                      f"(Left: '{self.revision_settings['left_delimiter']}'  "
                      f"Right: '{self.revision_settings['right_delimiter']}')"),
                fg="green")
        else:
            self.revision_settings['enabled'] = False
            self._revision_status.config(text="Revision extraction: Disabled", fg="gray")

    def _ask_save_paths(self) -> dict[str, str] | None:
        """Prompt user for save paths for each selected report. Returns None on cancel."""
        save_paths: dict[str, str] = {}
        for key, _label, default_name in self._REPORT_CONFIGS:
            if not self._report_vars[key].get():
                continue
            path = filedialog.asksaveasfilename(
                title=f"Save {default_name} file as...",
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx")],
                initialfile=default_name,
            )
            if not path:
                return None  # user cancelled
            save_paths[key] = path
        return save_paths

    def _start_processing(self) -> None:
        if not self.selected_folder:
            messagebox.showwarning("Warning", "Please select a PCF folder first!")
            return
        if not any(v.get() for v in self._report_vars.values()):
            messagebox.showwarning("Warning", "Please select at least one report to generate!")
            return

        save_paths = self._ask_save_paths()
        if save_paths is None:
            return

        progress = ProgressWindow(self.root)

        try:
            generate = {k: v.get() for k, v in self._report_vars.items()}

            result = self.processor.process_folder(
                self.selected_folder,
                progress_callback=progress.update_progress,
                save_paths=save_paths,
                include_continuation=self._var_continuation.get(),
                generate_reports=generate,
                revision_settings=self.revision_settings,
            )
            progress.close()

            if not result.success:
                messagebox.showwarning("Warning", result.error_message)
                return

            messagebox.showinfo("Success", PCFProcessor._build_summary(result))

            if save_paths and messagebox.askyesno("Open folder?", "Open results folder?"):
                self._open_folder(os.path.dirname(next(iter(save_paths.values()))))

        except PermissionError as exc:
            progress.close()
            messagebox.showerror(
                "File is Open",
                f"Cannot write file - it is open in another application.\n\n{exc}\n\n"
                f"Close the file and try again.",
            )
        except Exception as exc:
            progress.close()
            messagebox.showerror("Error", f"An error occurred:\n{exc}")

    # -- run ------------------------------------------------------------

    def run(self) -> None:
        """Start the Tk main loop."""
        self.root.mainloop()


def main() -> None:
    PCFProcessorApp().run()


if __name__ == "__main__":
    main()
