import os
import configparser
import logging
import re
import sys
import pickle
import threading
import hashlib
import copy
import gc
import time
import csv
import fnmatch
import traceback
from pathlib import Path
from collections import defaultdict
from UnionFind import UnionFind
from threading import Lock
from datetime import datetime
from dataclasses import dataclass, asdict, fields
from typing import Optional

@dataclass
class TransactionData:
    transaction_id: str
    date_min: Optional[datetime] = None
    date_max: Optional[datetime] = None
    priority: int = -1
    first_action: Optional[str] = None
    first_subcomponent: Optional[str] = None
    last_action: Optional[str] = None
    last_subcomponent: Optional[str] = None
    count_send: int = 0
    count_mnewtrans: int = 0
    date_in_collector: Optional[datetime] = None
    duration: float = 0.0
    duration_limsp: float = 0.0
    node_name: Optional[str] = None
    file_name: Optional[str] = None
    m_transaction_id: Optional[str] = None

class ProcessorFiles:
    # Constantes para los límites de memoria
    CHUNK_LIMIT = 0.9  # 90% del chunk_size
    

    def __init__(self, config_file):

        self.config = self.load_config(config_file)        
        self.csv_lock = threading.Lock()  # Lock para escritura en CSV
        if 'LOGGING' not in self.config:
            raise KeyError("'LOGGING' section not found in the configuration file.")        
        self.setup_logging()    
        self.total_lines = 0  # Contador para el número de registros en self.data_line
        self.pattern = None
        self.valid_actions = set()
        self.valid_subcomponents = set()
        self.countFiles = 0 #contador para la cantidad de archivos procesados (para efectos de log y progreso)
        self.totalFiles = 0 #Total de archivos a procesar, principalmente para efectos de log
        self.count_trx_complete = 0
        self.partition_buffers = defaultdict(list)  # buffer que contendrá los registros de cada partición
        self.uf = UnionFind()  # Instancia de Union-Find 
        self.csv_initialized = False  # Bandera para encabezado de CSV
        self.first_fo_records = {}  # Diccionario para almacenar los registros de FailOverManager "huerfanos"
 
                
        self.process = False #flag que indica que se ha comenzado a procesar los registros en data_line, 
                             #esto impide que log_process registre log con la cantidad de archivos que sería siempre la misma ya que mientras se está procesando data_line no se leen archivos

        self.count_incomplete_write = 0 # contador para conocer cuantos registros se han escrito al binario de descartadas  
        self.keep_running = True # establece cuando detener el hilo que escribe los logs

        self.load_configuration_values() 
        self.start_time = time.time()


    def load_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def setup_logging(self):
        log_level_str = self.config['LOGGING'].get('LogLevel', 'INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)

        # Logger principal con fecha en el nombre del archivo
        log_file_path = self.config['LOGGING']['LogFilePath']
        log_file_path_with_date = f"{os.path.splitext(log_file_path)[0]}_{datetime.now().strftime('%Y-%m-%d')}{os.path.splitext(log_file_path)[1]}"
        logging.basicConfig(filename=log_file_path_with_date,
                            level=log_level,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()

        # Segundo logger para procesamiento de archivos
        log_file_path_files = self.config['LOGGING']['LogFilePathFiles']
        log_file_path_files_with_date = f"{os.path.splitext(log_file_path_files)[0]}_{datetime.now().strftime('%Y-%m-%d')}{os.path.splitext(log_file_path_files)[1]}"
        file_handler = logging.FileHandler(log_file_path_files_with_date)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        self.loggerfiles = logging.getLogger('loggerfiles')
        self.loggerfiles.addHandler(file_handler)
        self.loggerfiles.setLevel(log_level)
        self.loggerfiles.propagate = False

    def load_configuration_values(self):
        if 'PROCESS_FILES' not in self.config:
            self.logger.error("'PROCESS_FILES' section not found in the configuration file.")
            raise KeyError("'PROCESS_FILES' section not found in the configuration file.")
       
        self.inputFile = self.config['PROCESS_FILES']['InputPath']        
        self.filePattern = self.config['PROCESS_FILES'].get('FilePattern', '*act*.log')
        self.IncompleteTransactionsFile = self.config['PROCESS_FILES']['IncompleteTransactionsFile']
                
        self.chunk_size = self.config['PROCESS_FILES'].getint('Chunk_size', 1000000)
        self.valid_actions = set(self.config['PROCESS_FILES']['valid_actions'].split(','))
        self.valid_subcomponents = set(self.config['PROCESS_FILES']['valid_subcomponents'].split(','))
        self.timeToLog = self.config['PROCESS_FILES'].getint('timeToLog', fallback=120)
        self.resultFinalFile = self.config['PROCESS_FILES']['ResultFinalFile']
        self.order_patterns = self.config['PROCESS_FILES'].get('file_order_patterns', 'limsp_adaptor*,limmsp_bus_massive*,limsp_bus*,limsp_collector*').split(',')
        self.num_partitions = self.config['PROCESS_FILES'].getint('num_partitions', 30)

        pattern = self.config['PROCESS_FILES'].get('log_pattern')
        if pattern:
            pattern = pattern.strip('"')
            pattern = pattern.replace('\\', '\\')
        
        self.log_pattern_str = pattern or r'\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)'

    def compile_regular_expression(self):
        try:
            self.pattern = re.compile(self.log_pattern_str)
            self.logger.debug(f"Expresión regular compilada: {self.log_pattern_str}")
        except re.error as e:
            self.logger.error(f"Error compilando expresión regular: {str(e)}")
            raise

    def orderbydate(self):
        files = [file for file in Path(self.inputFile).rglob(self.filePattern) if file.is_file()]
        order_patterns = [pattern.strip() for pattern in self.order_patterns]
        
        def sort_key(file):
            filename = file.name
            for i, pattern in enumerate(order_patterns):
                if fnmatch.fnmatch(filename, pattern):
                    return (i, file.stat().st_mtime)
            return (len(order_patterns), file.stat().st_mtime)

        return sorted(files, key=sort_key)

    def stable_hash(self,transaction_id: str) -> int:
        hash_obj = hashlib.md5(transaction_id.encode('utf-8'))
        return int(hash_obj.hexdigest(), 16)

    def get_partition(self,transaction_id: str, num_partitions: int) -> int:
        return self.stable_hash(transaction_id) % num_partitions
    
    def process_log_files(self):
   
        self.directory_path = Path(self.inputFile)
        
        if os.path.exists(self.IncompleteTransactionsFile):
            os.remove(self.IncompleteTransactionsFile)
            self.logger.warning(f'Archivo {self.IncompleteTransactionsFile} existe previamente. Se elimina antes de iniciar')
        
        matching_files = list(self.directory_path.rglob(self.filePattern)) 
        self.totalFiles = len(matching_files)

        if not matching_files:
            logging.error(f'No se encuentran archivo para procesar. {self.directory_path} archivos {self.filePattern}')
            sys.exit(1)
        
        self.logger.info(f'Archivos encontrados: {self.totalFiles}...')
        filesbydate = self.orderbydate()
        self.loggerfiles.info("Ordenado archivos:..")
        if self.logger.isEnabledFor(logging.DEBUG):
            for i, file_path in enumerate(filesbydate):
                self.loggerfiles.debug(f"{i+1}: {file_path}")
        self.loggerfiles.info("Ordenación completada")
        self.logger.info('Inicia procesamiento de archivos de logs')

        progress_thread = threading.Thread(target=self.log_progress, daemon=True)
        progress_thread.start()

        for file_path in filesbydate:
            self.countFiles += 1
            file_path = Path(file_path)
            node_name = file_path.parent.name
            file_name = file_path.name
            try:
                self.create_partitions(file_path, node_name, file_name, self.num_partitions)
            except Exception as e:
                error_details = traceback.format_exc()
                self.loggerfiles.error(f'Error processing file {file_name}:\n' + f'Error type: {type(e).__name__}\n' + f'Error message: {str(e)}\n' + f'Stack trace:\n{error_details}')
                self.loggerfiles.error(f'Estado actual:\n' + f'- Líneas procesadas: {self.total_lines}\n' + f'- Registros FailOver pendientes: {len(self.first_fo_records)}\n' + f'- Tamaño buffer particiones: {sum(len(buf) for buf in self.partition_buffers.values())}')
        
        self.logger.info('Se han procesado todos los archivos. Se escriben los registros restantes...')
        if self.first_fo_records:
            self.logger.info(f"{len(self.first_fo_records)} registros de FailOver quedaron sin relacionar")

        if any(self.partition_buffers.values()):
            self.flush_partition_buffers(self.num_partitions)
            
        self.logger.info('Se han creado todas las particiones. Se inicia el procesamiento de estas...')
        del self.uf, self.partition_buffers, self.first_fo_records
        gc.collect()

        self.keep_running = False
        progress_thread.join() 
  
    def log_progress(self):
        while self.keep_running:
            if not self.process:
                parcial_time = time.time()
                total_time = parcial_time - self.start_time
                progressFiles = (self.countFiles/self.totalFiles)*100
                total_fo_records = len(self.first_fo_records)
                self.logger.info(f"Total de archivos procesados {self.countFiles}, progreso {progressFiles:.2f}%. {total_fo_records} registros de FailOver sin relacionar")
                if total_time > 3600:
                    logging.info(f"Tiempo transcurrido: {total_time / 3600:.2f} horas.")
                else:
                    logging.info(f"Tiempo transcurrido: {total_time / 60:.2f} minutos.")
            time.sleep(self.timeToLog)

    def flush_partition_buffers(self, num_partitions):
        cant_partitions_written = 0
        self.logger.debug("Se inicia escritura de particiones en disco...")
        for partition_index in range(num_partitions):
            partition_file = f"{self.IncompleteTransactionsFile}_{partition_index}.bin"
            buffer = self.partition_buffers.get(partition_index, [])
            if buffer:
                cant_partitions_written += 1
                with open(partition_file, 'ab') as f:
                    pickle.dump(buffer, f)
                self.logger.debug(f"Se han escrito {len(buffer)} registros en la partición {partition_index}.")
                self.partition_buffers[partition_index] = []
        self.logger.info(f"Se han escrito {cant_partitions_written} particiones en disco.")

    def create_partitions(self, file_path, node_name, file_name, num_partitions: int):
        try:
            if not hasattr(self, 'partition_buffers'):
                self.partition_buffers = {i: [] for i in range(num_partitions)}
            
            if not os.access(file_path, os.R_OK):
                self.loggerfiles.error(f"No hay acceso de lectura al archivo: {file_path}")
                return

            for detail in self.log_file_generator(file_path):
                transaction_id = detail['transaction_id']
                if transaction_id is None:
                    self.logger.warning(f'Missing transaction_id in line from file: {file_path}. Details: {detail}')
                    continue

                action = detail['action']
                subcomponent = detail['subcomponent']

                if action == 'OUT' and subcomponent == 'FailOverManager' and self.uf.get_tree_size(transaction_id) == 0:
                    self.first_fo_records[transaction_id] = detail
                    continue
                
                self.uf.add_transaction(transaction_id)
                
                mtransaction_id = detail.get('Mtransaction_id')
                if mtransaction_id is not None:
                    self.uf.add_transaction(mtransaction_id)
                    self.uf.union(transaction_id, mtransaction_id)
                    if mtransaction_id in self.first_fo_records:
                        detail_fo = self.first_fo_records.pop(mtransaction_id)
                        canonical_id = self.uf.find(mtransaction_id) or transaction_id
                        detail_fo['transaction_id'] = canonical_id
                        self.total_lines += 1
                        detail_fo['nodename'] = node_name
                        detail_fo['filename'] = file_name
                        partition_index = self.get_partition(canonical_id, num_partitions)
                        self.partition_buffers[partition_index].append(detail_fo)

                elif action == 'SEND':
                    if transaction_id in self.first_fo_records:
                        detail_fo = self.first_fo_records.pop(transaction_id)
                        canonical_id = self.uf.find(transaction_id) or transaction_id
                        detail_fo['transaction_id'] = canonical_id
                        detail_fo['nodename'] = node_name
                        detail_fo['filename'] = file_name
                        self.total_lines += 1
                        partition_index = self.get_partition(canonical_id, num_partitions)
                        self.partition_buffers[partition_index].append(detail_fo)
                    elif self.uf.get_tree_size(transaction_id) == 1:
                        self.first_fo_records[transaction_id] = detail
                        continue

                canonical_id = self.uf.find(transaction_id) or transaction_id
                detail['transaction_id'] = canonical_id
                detail['nodename'] = node_name
                detail['filename'] = file_name
                self.total_lines += 1
                
                partition_index = self.get_partition(canonical_id, num_partitions)
                self.partition_buffers[partition_index].append(detail)

                if self.total_lines >= self.chunk_size * self.CHUNK_LIMIT:
                    self.logger.debug(f"Total de líneas alcanzadas {self.total_lines}. Se empiezan a procesar")
                    self.flush_partition_buffers(num_partitions)
                    self.total_lines = 0
        except Exception as e:
            self.loggerfiles.error(f'Error en UnionFind operations: Transaction ID: {transaction_id}, Error: {str(e)}')
            raise

    def process_partition_file(self, partition_file_path):
        data_line = defaultdict(list)
        if not os.path.exists(partition_file_path):
            self.logger.warning(f"La partición no existe: {partition_file_path}")
            return data_line
        try:
            self.logger.debug(f"Leyendo la partición: {partition_file_path}")
            with open(partition_file_path, 'rb') as f:
                while True:
                    try:
                        chunk = pickle.load(f)
                        for record in chunk:
                            if transaction_id := record.get('transaction_id'):
                                data_line[transaction_id].append(record)
                    except EOFError:
                        break
                    except pickle.UnpicklingError as e:
                        self.logger.error(f"Error al deserializar datos en {partition_file_path}: {str(e)}")
                        break
            return data_line
        except IOError as e:
            self.logger.error(f"Error de I/O al leer {partition_file_path}: {str(e)}")
            return data_line
        except Exception as e:
            self.logger.error(f"Error inesperado procesando {partition_file_path}: {str(e)}")
            return data_line

    def get_partition_file_names(self, num_partitions: int) -> list:
        return [f"{self.IncompleteTransactionsFile}_{i}.bin" for i in range(num_partitions)]

    def _get_duration_seconds(self, start_date: Optional[datetime], end_date: Optional[datetime]) -> float:
        if start_date and end_date:
            return (end_date.replace(microsecond=0) - start_date.replace(microsecond=0)).total_seconds()
        return 0.0

    def _process_single_transaction(self, transaction_id, records, pattern_mtrx):
        records_multisend = defaultdict(lambda: defaultdict(dict))
        has_input_record = False
        has_output_record = False
        has_flowctrl = False
        
        result = TransactionData(transaction_id=transaction_id)

        for record in records:
            timestamp = record['timestamp']
            action = record['action']
            subcomponent = record['subcomponent']
            mtransaction_id = record.get('Mtransaction_id')

            if mtransaction_id:
                result.count_mnewtrans += 1
                if result.count_mnewtrans > 1:
                    records_multisend[transaction_id][mtransaction_id].update({'m_transaction_id': mtransaction_id})
                else:
                    result.m_transaction_id = mtransaction_id
            else:
                if match_mtrx := pattern_mtrx.search(record.get('details', '')):
                    mtransaction_id = match_mtrx.group(1)

            if (priority := record.get('priority', -1)) != -1:
                result.priority = priority

            if action == 'NEWTRANS':
                result.date_min = timestamp
                result.first_action = action
                result.first_subcomponent = subcomponent
                result.node_name = record.get('nodename')
                result.file_name = record.get('filename')
                has_input_record = True
                continue

            if action == 'SEND':
                result.count_send += 1
                if mtransaction_id == result.m_transaction_id:
                    result.date_max = timestamp
                    result.last_action = action
                    result.last_subcomponent = subcomponent
                    has_output_record = True
                else:
                    records_multisend[transaction_id][mtransaction_id].update({
                        'transaction_id': transaction_id,
                        'date_max': timestamp,
                        'last_action': action,
                        'last_subcomponent': subcomponent
                    })
                continue

            if action == 'OUT' and subcomponent == 'FailOverManager':
                has_flowctrl = True
                if mtransaction_id == result.m_transaction_id or result.count_mnewtrans == 0:
                    result.date_in_collector = timestamp
                else:
                    records_multisend[transaction_id][mtransaction_id].update({'date_in_collector': timestamp})
                continue
        
        if transaction_id in records_multisend:
            m_records_dict = records_multisend[transaction_id]
            m_ids_to_delete = [m_id for m_id, record_data in m_records_dict.items() if 'date_max' not in record_data]
            for m_id in m_ids_to_delete:
                del m_records_dict[m_id]

        if has_input_record and has_output_record:
            result.duration = self._get_duration_seconds(result.date_min, result.date_max)
            if has_flowctrl:
                result.duration_limsp = self._get_duration_seconds(result.date_min, result.date_in_collector)
            return asdict(result), records_multisend
        elif has_input_record:
            result.last_action = 'KO'
            result.last_subcomponent = 'KO'
            return asdict(result), records_multisend

        return None, records_multisend

    def _calculate_multisend_durations(self, records_multisend, records_complete):
        if not records_multisend:
            return

        trx_data = {record['transaction_id']: {'date_min': record['date_min']} for record in records_complete}

        for transaction_id, m_records_dict in records_multisend.items():
            if transaction_id not in trx_data:
                self.logger.warning(f"Transaction ID {transaction_id} not found in trx_data. Skipping multisend record.")
                continue

            date_min = trx_data[transaction_id]['date_min']
            for record_data in m_records_dict.values():
                try:
                    date_max = record_data.get('date_max')
                    date_in_collector = record_data.get('date_in_collector')

                    if isinstance(date_max, str):
                        date_max = datetime.strptime(date_max, "%Y/%m/%d %H:%M:%S.%f")
                    if isinstance(date_in_collector, str):
                        date_in_collector = datetime.strptime(date_in_collector, "%Y/%m/%d %H:%M:%S.%f")

                    record_data['duration'] = self._get_duration_seconds(date_min, date_max)
                    record_data['duration_limsp'] = self._get_duration_seconds(date_min, date_in_collector)
                
                except (TypeError, ValueError) as e:
                    self.logger.error(f"Could not calculate duration for record {record_data.get('m_transaction_id')}: {e}")
                    record_data['duration'] = 0
                    record_data['duration_limsp'] = 0

    def process_transactions(self, data_line):
        records_multisend_total = defaultdict(lambda: defaultdict(dict))
        records_complete = []
        pattern_mtrx = re.compile(r"transaction:(\S+)")

        for transaction_id, records in data_line.items():
            result, records_multisend_single = self._process_single_transaction(transaction_id, records, pattern_mtrx)
            
            if result:
                records_complete.append(result)

            if records_multisend_single:
                for m_id, m_records in records_multisend_single.get(transaction_id, {}).items():
                    records_multisend_total[transaction_id][m_id].update(m_records)

        self._calculate_multisend_durations(records_multisend_total, records_complete)

        return records_complete, records_multisend_total
    
    def write_in_threads(self, processed_data, records_multisend):
        threads = []
        thread_complete = threading.Thread(target=self.write_result_to_csv, args=(processed_data,), daemon=True)
        threads.append(thread_complete)

        if records_multisend:
            thread_multisend = threading.Thread(target=self.write_multisend_to_csv, args=(records_multisend,), daemon=True)
            threads.append(thread_multisend)

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def write_result_to_csv(self, records_complete):
        try:
            if not records_complete:
                self.logger.debug('No hay transacciones completas para escribir.')
                return
            
            self.logger.debug(f"Se inicia escritura de transacciones al archivo binario. {len(records_complete)}")
            fieldnames = [f.name for f in fields(TransactionData)]
            
            with self.csv_lock:
                with open(self.resultFinalFile, 'a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
                    if not self.csv_initialized:
                        writer.writeheader()
                        self.csv_initialized = True
                    writer.writerows(records_complete)

                self.logger.info(f"{len(records_complete)} transacciones escritas en CSV")
                self.count_trx_complete += len(records_complete)

        except Exception as e:
            self.logger.error(f"Error al escribir CSV: {e}")

    def write_multisend_to_csv(self, records_multisend):
        try:
            if not records_multisend:
                self.logger.debug('No hay transacciones multisend para escribir.')
                return
                
            multisend_file = self.resultFinalFile.replace('.csv', '_multisend.csv')
            self.logger.debug(f"Se inicia escritura de transacciones multisend. Total IDs: {len(records_multisend)}")

            csv_headers = [f.name for f in fields(TransactionData)]
            rows_written = 0

            with self.csv_lock:
                with open(multisend_file, 'a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=csv_headers)
                    if csv_file.tell() == 0:
                        writer.writeheader()

                    for transaction_id, m_records_dict in records_multisend.items():
                        for m_id, record_data in m_records_dict.items():
                            record_data['m_transaction_id'] = m_id
                            writer.writerow(record_data)
                            rows_written += 1

            self.logger.info(f"Registros (filas) multisend escritos: {rows_written}")

        except Exception as e:
            self.logger.error(f"Error escribiendo registros multisend: {str(e)}")

    def log_file_generator(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line_num, line in enumerate(file, 1):
                    try:
                        if processed_line := self.process_log_line(line):
                            yield processed_line
                    except Exception as e:
                        self.loggerfiles.error(f'Error procesando línea {line_num} en {file_path}:\nLínea: {line[:200]}...\nError: {str(e)}')
        except Exception as e:
            self.loggerfiles.error(f'Error abriendo archivo {file_path}:\nError: {str(e)}')
            raise

    def process_log_line(self, line):
        try:
            if not (match := self.pattern.match(line)):
                return None

            details = match.groupdict()
            action = details['action']
            subcomponent = details['subcomponent']
            
            if action not in self.valid_actions and (subcomponent not in self.valid_subcomponents or action != 'OUT'):
                return None

            transaction_matches = list(re.finditer(r"transaction:([^ ]*)", details['details']))
            if not transaction_matches:
                return None

            details['transaction_id'] = transaction_matches[0].group(1)
            if len(transaction_matches) > 1:
                details['Mtransaction_id'] = transaction_matches[1].group(1)
            else:
                details['Mtransaction_id'] = None

            if priority_match := re.search(r"pri:(\d+)", details['details']):
                details['priority'] = int(priority_match.group(1))
            else:
                details['priority'] = -1

            timestamp_str = details['timestamp']
            if '.' in timestamp_str:
                details['timestamp'] = datetime.strptime(timestamp_str, "%Y/%m/%d %H:%M:%S.%f")
            else:
                details['timestamp'] = datetime.strptime(timestamp_str, "%Y/%m/%d %H:%M:%S")

            return details
        except Exception as e:
            self.loggerfiles.error(f'Error en process_log_line:\nLínea: {line[:200]}...\nError: {str(e)}')
            return None

    def write_dataconfig(self):
        self.logger.info("VERSION 7.0.0")
        self.logger.info(f"inputPath: {self.inputFile}")
        self.logger.info(f"filePattern: {self.filePattern}")
        self.logger.info(f"ResultFinalFile: {self.resultFinalFile}")
        self.logger.info(f"Binarios: {self.IncompleteTransactionsFile}")
        self.logger.info(f"chunk_size: {self.chunk_size}")
        self.logger.info(f"timeToLog: {self.timeToLog}")
        self.logger.info(f"num_partitions: {self.num_partitions}")
        self.logger.info(f"valid_actions: {self.valid_actions}")
        self.logger.info(f"valid_subcomponents: {self.valid_subcomponents}")
        self.logger.info(f"file_order_patterns: {self.order_patterns}")
        self.logger.info(f"log_pattern: {self.log_pattern_str}")

if __name__ == "__main__":
    try:
        manager = ProcessorFiles('./config/config.ini')
        logging.info("Iniciando proceso..")
        manager.compile_regular_expression()
        manager.write_dataconfig()
        manager.process_log_files()
        
        partition_file_names = manager.get_partition_file_names(manager.num_partitions)
        partitions_not_found = 0
        partitions_processed = 0
        
        for partition_file in partition_file_names:
            try:
                if not os.path.exists(partition_file):
                    partitions_not_found += 1
                    manager.logger.warning(f"Partición no encontrada: {partition_file}")
                    continue
                
                data_line = manager.process_partition_file(partition_file)
                if not data_line:
                    manager.logger.debug(f"Partición vacía: {partition_file}")
                    continue
                
                processed_data, records_multisend = manager.process_transactions(data_line)
                if processed_data:
                    manager.logger.debug(f"Partición {partition_file} procesada. Inicia proceso de escritura")
                    manager.write_in_threads(processed_data, records_multisend)
                    partitions_processed += 1
                
                del data_line, processed_data, records_multisend
                gc.collect()

            except Exception as e:
                manager.logger.error(f"Error procesando partición {partition_file}: {str(e)}")

        manager.logger.info("Resumen de particiones:")
        manager.logger.info(f"- Particiones procesadas: {partitions_processed}")
        manager.logger.info(f"- Particiones no encontradas: {partitions_not_found}")
        manager.logger.info(f"- Particiones totales esperadas: {manager.num_partitions}")
        
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if 'manager' in locals() and manager.start_time:
            end_time = time.time()
            total_time = end_time - manager.start_time
            logging.info(f"Se escriben {manager.count_trx_complete} transacciones completas")
            logging.info(f"Se descartan {manager.count_incomplete_write} transacciones")
            if total_time > 3600:
                logging.info(f"Tiempo total: {total_time / 3600:.2f} horas.")
            else:
                logging.info(f"Tiempo total: {total_time / 60:.2f} minutos.")