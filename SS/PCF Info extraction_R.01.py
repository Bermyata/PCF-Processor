#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PCF Processor - Обработчик PCF файлов
Создает две Excel таблицы из данных PCF файлов:
1. ВСЕ данные блока PIPELINE-REFERENCE (динамически определяются все столбцы)
2. Информация об инструментах (TAG поля из блоков INSTRUMENT)

Автор: PCF Data Processor
"""

import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import re
from pathlib import Path
import logging
from datetime import datetime

class PCFProcessor:
    def __init__(self):
        self.setup_logging()
        
    def setup_logging(self):
        """Настройка логирования"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('pcf_processor.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    def parse_pcf_file(self, file_path):
        """
        Парсинг одного PCF файла
        Возвращает tuple: (pipeline_data, instruments)
        """
        try:
            # Пробуем разные кодировки
            for encoding in ['latin1', 'utf-8', 'cp1252', 'iso-8859-1']:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        content = f.read()
                    break
                except UnicodeDecodeError:
                    continue
            else:
                self.logger.error(f"Не удалось прочитать файл с поддерживаемыми кодировками: {file_path}")
                return None, None
                    
        except Exception as e:
            self.logger.error(f"Ошибка чтения файла {file_path}: {e}")
            return None, None
        
        pipeline_data = {}
        instruments = []
        
        # Извлекаем весь блок PIPELINE-REFERENCE до первого блока компонента
        lines = content.split('\n')
        in_pipeline_block = False
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
                
            # Начинаем сбор данных после того, как встретили PIPELINE-REFERENCE
            if line.startswith('PIPELINE-REFERENCE'):
                in_pipeline_block = True
                # Извлекаем значение PIPELINE-REFERENCE
                parts = line.split(None, 1)  # Разделяем на максимум 2 части
                if len(parts) > 1:
                    pipeline_data['PIPELINE-REFERENCE'] = parts[1]
                continue
            
            # Если мы в блоке pipeline, собираем все данные
            if in_pipeline_block:
                # Блоки компонентов - прекращаем сбор pipeline данных при их обнаружении
                component_blocks = [
                    'FLANGE-BLIND', 'GASKET', 'BOLT', 'INSTRUMENT', 'PIPE', 'ELBOW', 
                    'TEE', 'REDUCER', 'VALVE', 'OLET', 'SUPPORT', 'COMPONENT-IDENTIFIER',
                    'END-POINT', 'CENTRE-POINT', 'SKEY', 'MATERIAL-IDENTIFIER'
                ]
                
                # Если строка - это начало блока компонента, прекращаем сбор pipeline данных
                if any(line.startswith(block) for block in component_blocks):
                    break
                
                # Если строка содержит пробелы, это параметр со значением
                if ' ' in line:
                    parts = line.split(None, 1)  # Разделяем на максимум 2 части
                    if len(parts) == 2:
                        key = parts[0]
                        value = parts[1]
                        pipeline_data[key] = value
        
        # Извлекаем блоки INSTRUMENT и их TAG поля
        instrument_pattern = r'INSTRUMENT\s*\n(.*?)(?=\n[A-Z-]+(?:\s|\n)|\nEND-POSITION|\nCONNECTION-REFERENCE|\nMATERIALS|\Z)'
        instrument_blocks = re.finditer(instrument_pattern, content, re.DOTALL)
        
        for block in instrument_blocks:
            block_content = block.group(1)
            # Ищем TAG поле в блоке
            tag_match = re.search(r'TAG\s+(.+)', block_content, re.MULTILINE)
            if tag_match:
                tag_value = tag_match.group(1).strip()
                instruments.append(tag_value)
        
        return pipeline_data, instruments
    
    def process_folder(self, folder_path, progress_callback=None):
        """
        Обрабатывает все PCF файлы в папке и создает две таблицы Excel
        ВАЖНО: Динамически определяет ВСЕ уникальные параметры из всех файлов
        """
        # Поиск PCF файлов
        pcf_files = []
        for root, dirs, files in os.walk(folder_path):
            for file in files:
                if file.lower().endswith('.pcf'):
                    pcf_files.append(os.path.join(root, file))
        
        if not pcf_files:
            messagebox.showwarning("Предупреждение", "В выбранной папке не найдено PCF файлов!")
            return False
        
        self.logger.info(f"Найдено {len(pcf_files)} PCF файлов для обработки")
        
        # Инициализация данных
        table1_data = []  # PIPELINE-REFERENCE данные
        table2_data = []  # INSTRUMENT теги
        all_pipeline_keys = set()  # Собираем ВСЕ уникальные ключи из ВСЕХ файлов
        
        processed_files = 0
        errors = 0
        
        # ПЕРВЫЙ ПРОХОД - определяем все возможные ключи pipeline блока ИЗ ВСЕХ ФАЙЛОВ
        self.logger.info("Первый проход - анализ структуры всех файлов для определения всех столбцов...")
        for i, file_path in enumerate(pcf_files):
            if progress_callback:
                progress_callback(i, len(pcf_files) * 2, f"Анализ файла {i+1}/{len(pcf_files)}: {os.path.basename(file_path)}")
                
            pipeline_data, instruments = self.parse_pcf_file(file_path)
            if pipeline_data:
                # Добавляем все найденные ключи в общий набор
                all_pipeline_keys.update(pipeline_data.keys())
        
        self.logger.info(f"Найдено {len(all_pipeline_keys)} уникальных параметров во всех файлах")
        
        # ВТОРОЙ ПРОХОД - сбор данных с использованием всех найденных столбцов
        self.logger.info("Второй проход - сбор данных...")
        for i, file_path in enumerate(pcf_files):
            if progress_callback:
                progress_callback(len(pcf_files) + i, len(pcf_files) * 2, f"Обработка файла {i+1}/{len(pcf_files)}: {os.path.basename(file_path)}")
                
            pipeline_data, instruments = self.parse_pcf_file(file_path)
            
            if pipeline_data is not None:
                # Данные для первой таблицы
                row_data = {
                    'Имя файла': os.path.basename(file_path),
                    'Полный путь': file_path
                }
                
                # Добавляем ВСЕ ключи из pipeline блока (найденные во ВСЕХ файлах)
                # Если параметр отсутствует в текущем файле - ставим пустую строку
                for key in sorted(all_pipeline_keys):
                    row_data[key] = pipeline_data.get(key, '')  # Пустая строка если параметра нет в этом файле
                
                table1_data.append(row_data)
                
                # Данные для второй таблицы
                if instruments:
                    for instrument in instruments:
                        table2_data.append({
                            'Имя файла': os.path.basename(file_path),
                            'Полный путь': file_path,
                            'INSTRUMENT': instrument
                        })
                else:
                    # Если инструментов нет, добавляем строку с пустым INSTRUMENT
                    table2_data.append({
                        'Имя файла': os.path.basename(file_path),
                        'Полный путь': file_path,
                        'INSTRUMENT': ''
                    })
                
                processed_files += 1
            else:
                errors += 1
                self.logger.warning(f"Не удалось обработать файл: {file_path}")
        
        # Создание DataFrame и сохранение в Excel
        try:
            df1 = pd.DataFrame(table1_data)
            df2 = pd.DataFrame(table2_data)
            
            # Создаем имена файлов с временной меткой
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path1 = os.path.join(folder_path, f'PCF_Pipeline_Attributes_{timestamp}.xlsx')
            output_path2 = os.path.join(folder_path, f'PCF_Instruments_{timestamp}.xlsx')
            
            # Сохранение с дополнительными опциями форматирования
            with pd.ExcelWriter(output_path1, engine='openpyxl') as writer:
                df1.to_excel(writer, index=False, sheet_name='Pipeline_Attributes')
                
                # Автоширина столбцов
                worksheet = writer.sheets['Pipeline_Attributes']
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
            
            with pd.ExcelWriter(output_path2, engine='openpyxl') as writer:
                df2.to_excel(writer, index=False, sheet_name='Instruments')
                
                # Автоширина столбцов
                worksheet = writer.sheets['Instruments']
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
            
            # Подсчет статистики
            total_instruments = len([x for x in table2_data if x['INSTRUMENT']])
            
            self.logger.info(f"Обработка завершена успешно!")
            self.logger.info(f"Обработано файлов: {processed_files}")
            self.logger.info(f"Ошибок: {errors}")
            self.logger.info(f"Найдено инструментов: {total_instruments}")
            self.logger.info(f"Найдено уникальных параметров pipeline: {len(all_pipeline_keys)}")
            
            # Список всех найденных параметров для пользователя
            pipeline_params_list = '\n'.join([f"• {key}" for key in sorted(all_pipeline_keys)])
            
            message = (f"Обработка завершена!\n\n"
                      f"Статистика:\n"
                      f"• Обработано файлов: {processed_files}\n"
                      f"• Ошибок: {errors}\n"
                      f"• Найдено инструментов: {total_instruments}\n"
                      f"• Найдено уникальных параметров pipeline: {len(all_pipeline_keys)}\n\n"
                      f"Созданы файлы:\n"
                      f"• {os.path.basename(output_path1)}\n"
                      f"• {os.path.basename(output_path2)}\n\n"
                      f"Найденные параметры pipeline:\n{pipeline_params_list}")
            
            messagebox.showinfo("Успешно", message)
            return True
            
        except Exception as e:
            self.logger.error(f"Ошибка при сохранении файлов: {e}")
            messagebox.showerror("Ошибка", f"Ошибка при сохранении файлов: {e}")
            return False

class ProgressWindow:
    def __init__(self, parent):
        self.window = tk.Toplevel(parent)
        self.window.title("Обработка файлов...")
        self.window.geometry("500x180")
        self.window.resizable(False, False)
        self.window.transient(parent)
        self.window.grab_set()
        
        # Центрирование окна
        self.window.update_idletasks()
        x = (self.window.winfo_screenwidth() // 2) - (500 // 2)
        y = (self.window.winfo_screenheight() // 2) - (180 // 2)
        self.window.geometry(f"500x180+{x}+{y}")
        
        self.label = tk.Label(self.window, text="Подготовка...", pady=10)
        self.label.pack()
        
        self.progress = ttk.Progressbar(self.window, mode='determinate', length=450)
        self.progress.pack(pady=10)
        
        self.status_label = tk.Label(self.window, text="", wraplength=450)
        self.status_label.pack()
        
    def update_progress(self, current, total, status):
        if total > 0:
            percentage = (current / total) * 100
            self.progress['value'] = percentage
            self.label.config(text=f"Прогресс: {current}/{total} ({percentage:.1f}%)")
            self.status_label.config(text=status)
        self.window.update()
        
    def close(self):
        self.window.destroy()

def main():
    """Главная функция приложения"""
    processor = PCFProcessor()
    
    # Создаем главное окно
    root = tk.Tk()
    root.title("PCF Processor - Динамический анализ")
    root.geometry("600x350")
    root.resizable(False, False)
    
    # Центрирование главного окна
    root.update_idletasks()
    x = (root.winfo_screenwidth() // 2) - (600 // 2)
    y = (root.winfo_screenheight() // 2) - (350 // 2)
    root.geometry(f"600x350+{x}+{y}")
    
    # Заголовок
    title_label = tk.Label(root, text="PCF File Processor", font=("Arial", 16, "bold"))
    title_label.pack(pady=20)
    
    description = tk.Label(root, text="Обработчик PCF файлов с динамическим анализом параметров\n\n"
                                     "Создает две Excel таблицы:\n"
                                     "1. ВСЕ данные блока PIPELINE-REFERENCE\n"
                                     "   (автоматически определяет все параметры из всех файлов)\n"
                                     "2. Информация об инструментах (TAG поля)\n\n"
                                     "⚡ Параметры из разных файлов объединяются в одну таблицу",
                          justify="center", wraplength=550)
    description.pack(pady=20)
    
    def select_and_process():
        folder_path = filedialog.askdirectory(
            title="Выберите папку с PCF файлами для обработки"
        )
        
        if folder_path:
            # Создаем окно прогресса
            progress_window = ProgressWindow(root)
            
            def progress_callback(current, total, status):
                progress_window.update_progress(current, total, status)
            
            # Запускаем обработку
            try:
                success = processor.process_folder(folder_path, progress_callback)
                progress_window.close()
                
                if success:
                    # Предлагаем открыть папку с результатами
                    if messagebox.askyesno("Открыть папку?", "Хотите открыть папку с результатами?"):
                        os.startfile(folder_path)
                        
            except Exception as e:
                progress_window.close()
                messagebox.showerror("Ошибка", f"Произошла ошибка: {e}")
        
    # Кнопка выбора папки
    select_button = tk.Button(root, text="Выбрать папку и начать обработку", 
                             command=select_and_process, 
                             font=("Arial", 12), 
                             bg="#4CAF50", fg="white", 
                             padx=20, pady=10)
    select_button.pack(pady=30)
    
    # Информация внизу
    info_label = tk.Label(root, text="Поддерживаемые форматы: .pcf\n"
                                    "Поиск файлов рекурсивно во всех подпапках\n"
                                    "Динамическое определение всех параметров",
                         font=("Arial", 9), fg="gray")
    info_label.pack(side="bottom", pady=10)
    
    # Запуск GUI
    root.mainloop()

if __name__ == "__main__":
    main()
