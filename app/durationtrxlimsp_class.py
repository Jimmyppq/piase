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
from pathlib import Path
from collections import defaultdict
from UnionFind import UnionFind
from threading import Lock
from datetime import datetime


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
        self.node_name = str()
        self.file_name = str()
        self.countFiles = 0 #contador para la cantidad de archivos procesados (para efectos de log y progreso)
        self.totalFiles = 0 #Total de archivos a procesar, principalmente para efectos de log
        self.count_complete_fromprevious = 0
        self.count_trx_complete = 0
        self.partition_buffers = defaultdict(list)  # buffer que contendrá los registros de cada partición
        self.uf = UnionFind()  # Instancia de Union-Find 
        self.csv_initialized = False  # Bandera para encabezado de CSV

 
                
        self.process = False #flag que indica que se ha comenzado a procesar los registros en data_line, 
                             #esto impide que log_process registre log con la cantidad de archivos que sería siempre la misma ya que mientras se está procesando data_line no se leen archivos

        #self.data_line = []
        #self.data_line = defaultdict(list)
        self.count_incomplete_write = 0 # contador para conocer cuantos registros se han escrito al binario de descartadas  
        #self.records_complete = [] # Lista para almacenar los resultados finales
        self.records_incomplete = {} # Diccionario para almacenar las transacciones que aun no son finales pero que podrian tener un estado final en otro bloque de procesamiento.
        '''self.global_transactions = defaultdict(lambda: {
            'date_min': None,
            'date_max': None,
            'priority': -1,
            'first_action': None,
            'last_action': None,
            'first_subcomponent': None,
            'last_subcomponent': None,
            'collector_times': [],
            'mnewtrans': None,
            'countSend': 0,
            'send_times': [],
            'Duration': None,
            'duration_limsp': None
        })'''
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
        self.CompletedTransactionsFile = self.config['PROCESS_FILES']['CompletedTransactionsFile']        
        self.chunk_size = self.config['PROCESS_FILES'].getint('Chunk_size', 1000000)
        #Se establece como límite de seguridad el 10% del chunk para utilizarlo como registros incompletos
        # para procesar en siguientes ciclos de "data_line"
        self.mem_trx_security = self.chunk_size * 0.1
        #self.mem_trx_security = self.config['PROCESS_FILES'].getint('mem_trx_security', 5000000)        
        self.valid_actions = set(self.config['PROCESS_FILES']['valid_actions'].split(','))
        self.valid_subcomponents = set(self.config['PROCESS_FILES']['valid_subcomponents'].split(','))
        # tienmpo en segundos que se ejecutará el hilo que registra actividad en los logs
        self.timeToLog = self.config['PROCESS_FILES'].getint('timeToLog', fallback=120)
        self.resultFinalFile = self.config['PROCESS_FILES']['ResultFinalFile']

    def compile_regular_expression(self):
        self.pattern = re.compile(r"\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)")

    def process_log_line(self, line):
        transaction_pattern = r"(transaction:)([^ ]*)"
        priority_pattern = r"pri:(\d+)"

        match = self.pattern.match(line)

        if match:
            details = match.groupdict()
            action = details['action']
            subcomponent = details['subcomponent']
            if action not in self.valid_actions and subcomponent not in self.valid_subcomponents:
                return None

            transaction_matches = re.finditer(transaction_pattern, details['details'])
            transaction_ids = [m.group(2) for m in transaction_matches]

            details['transaction_id'] = transaction_ids[0] if transaction_ids else None
            details['Mtransaction_id'] = transaction_ids[1] if len(transaction_ids) >= 2 else None

            priority_match = re.search(priority_pattern, details['details'])
            details['priority'] = int(priority_match.group(1)) if priority_match else -1

            if '.' in details['timestamp']:
                details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
            else:
                details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S")

            return details

        return None

    def orderbydate(self):
        """
        Ordena los archivos según un orden de patrones configurable y luego por fecha de modificación.
        """
        files = [file for file in Path(self.inputFile).rglob(self.filePattern) if file.is_file()]        
        
        # Obtener el orden de los patrones desde la configuración
        order_patterns = self.config['PROCESS_FILES'].get('file_order_patterns', 'limsp_adaptor*,limmsp_bus_massive*,limsp_bus*,limsp_collector*').split(',')
        order_patterns = [pattern.strip() for pattern in order_patterns]  # Eliminar espacios en blanco

        def sort_key(file):
            filename = file.name
            for i, pattern in enumerate(order_patterns):
                if fnmatch.fnmatch(filename, pattern):
                    return (i, file.stat().st_mtime)  # Prioridad por patrón, luego por fecha
            return (len(order_patterns), file.stat().st_mtime)  # Si no coincide, al final, ordenado por fecha

        files_sorted = sorted(files, key=sort_key)        
        return files_sorted        

    def stable_hash(self,transaction_id: str) -> int:
        """
        Calcula un hash estable para un transaction_id utilizando MD5.
        Se convierte el hash hexadecimal en un entero.
        """
        # Convertir el transaction_id a bytes y calcular el hash MD5
        hash_obj = hashlib.md5(transaction_id.encode('utf-8'))
        # Convertir el hash en hexadecimal a un entero (base 16)
        return int(hash_obj.hexdigest(), 16)

    def get_partition(self,transaction_id: str, num_partitions: int) -> int:
        """
        Retorna el índice de partición para un transaction_id dado el número total de particiones.
        """
        return self.stable_hash(transaction_id) % num_partitions
    
    def process_log_files(self):   
        
        self.directory_path = Path(self.inputFile)
        num_partitions = 30
        
        # Verificar si los archivos existen y eliminarlos        
        if os.path.exists(self.IncompleteTransactionsFile):
            os.remove(self.IncompleteTransactionsFile)
            self.logger.warning(f'Archivo {self.IncompleteTransactionsFile} existe previamente. Se elimina antes de iniciar')        
        if os.path.exists(self.CompletedTransactionsFile):
            os.remove(self.CompletedTransactionsFile)
            self.logger.warning(f'Archivo {self.CompletedTransactionsFile} existe previamente. Se elimina antes de iniciar') 
        
        matching_files = list(self.directory_path.rglob(self.filePattern)) 
        self.totalFiles = len(matching_files)

        if not matching_files:
            logging.error(f'No se encuentran archivo para procesar. {self.directory_path} archivos {self.filePattern}')
            sys.exit(1)
        
        self.logger.info(f'Archivos encontrados: {self.totalFiles}...')
        filesbydate = self.orderbydate()
        self.loggerfiles.info("Ordenado archivos por fecha de modificación:..")
        if self.logger.isEnabledFor(logging.DEBUG):
            for i, file_path in enumerate(filesbydate):
                self.loggerfiles.debug(f"{i+1}: {file_path}")
        self.loggerfiles.info("Ordenación completada")
        self.logger.info('Inicia procesamiento de archivos de logs')

        progress_thread = threading.Thread(target=self.log_progress, daemon=True)
        progress_thread.start()

        for file_path in filesbydate:
            self.countFiles +=1
            file_path = Path(file_path)
            node_name = file_path.parent.name
            file_name = file_path.name
            path, file_name = os.path.split(file_path)
            self.loggerfiles.debug(f'Nodo: {node_name} -- Archivo ({self.countFiles}): {file_name} -- Path: {path}')   
            try:
                #self.process_transactions(file_path,node_name,file_name)
                self.create_partitions(file_path,node_name,file_name, num_partitions)
            except Exception as e:
                self.loggerfiles.error(f'Error processing file {file_name}: {e}')
        
        # Escribir los datos restantes al final del archivo
        self.logger.info('Se han procesado todos los archivos. Se escriben los registros restantes...')
        if any(self.partition_buffers.values()):
            self.flush_partition_buffers(num_partitions)

        self.logger.info('Se han creado todas las particiones. Se inicia el procesamiento de estas...')
        del self.uf  # Liberar memoria de la estructura Union-Find
        del self.partition_buffers  # Liberar memoria de los buffers de particiones
        gc.collect()  # Forzar la recolección de basura

        self.keep_running = False
        progress_thread.join()
  
    def log_progress(self):
        while self.keep_running:
            if not self.process :
                parcial_time = time.time()
                total_time = parcial_time - self.start_time
                progressFiles = (self.countFiles/self.totalFiles)*100
                self.logger.info(f"Total de archivos procesados {self.countFiles}, progreso {progressFiles:.2f}%")
                if total_time > 3600 :
                    logging.info(f"Tiempo transcurrido: {total_time / 3600:.2f} horas.")
                else:
                    logging.info(f"Tiempo transcurrido: {total_time / 60:.2f} minutos.")
            time.sleep(self.timeToLog)  # Esperar x segundos                               

    def flush_partition_buffers(self, num_partitions):
        cant_partitions_written = 0
        self.logger.debug("Se inicia escritura de particiones en disco...")
        for partition_index in range(num_partitions):
            partition_file = f"{self.IncompleteTransactionsFile}_{partition_index}.bin"
            buffer = self.partition_buffers.get(partition_index, [])
            if buffer:
                cant_partitions_written += 1
                # Escribe el buffer en bloque
                with open(partition_file, 'ab') as f:
                    pickle.dump(buffer, f)
                self.logger.debug(f"Se han escrito {len(buffer)} registros en la partición {partition_index}.")
                # Vacía el buffer
                self.partition_buffers[partition_index] = []

        self.logger.info(f"Se han escrito {cant_partitions_written} particiones en disco.")

    def create_partitions(self,file_path,node_name,file_name, num_partitions: int):
        
        if not hasattr(self, 'partition_buffers'):
            self.partition_buffers = {i: [] for i in range(num_partitions)}

        for detail in self.log_file_generator(file_path):
            transaction_id = detail['transaction_id']
            if transaction_id is None:
                self.logger.warning(f'Missing transaction_id in line from file: {file_path}. Details: {detail}')
                continue
            
            
            self.uf.add_transaction(transaction_id)
            mtransaction_id = detail.get('Mtransaction_id')
            if mtransaction_id is not None:
                self.uf.add_transaction(mtransaction_id)
                self.uf.union(transaction_id, mtransaction_id)
            
            #canonical transaction id
            canonical_id = self.uf.find(transaction_id)
            detail['transaction_id'] = canonical_id

            self.total_lines += 1
            detail['nodename'] = node_name
            detail['filename'] = file_name
            
            partition_index = self.get_partition(canonical_id, num_partitions)            
            self.partition_buffers[partition_index].append(detail)

            # Verificar si la cantidad de líneas en memoria es mayor al 90% del tamaño del chunk.
            # Dado que esta validación se hace al final del procesamiento del for (es decir de un archivo completo).
            # Al validar por encima del 80% se puede llegar a tener en memoria una cantidad de líneas superior al 90% e incluso
            # si un archivo fuera lo suficientemente grande un valor cercano o superior al 100%
            if self.total_lines >= self.chunk_size * self.CHUNK_LIMIT:
                self.logger.debug(f"Total de líneas alcanzadas {self.total_lines}. Se empiezan a procesar")
                self.flush_partition_buffers(num_partitions)
                # Reiniciar el contador global y vaciar buffers de data_line si procede
                self.total_lines = 0
                # Dependiendo de la lógica, podrías limpiar también self.data_line o mantener las transacciones incompletas.
        
   
               
    def process_partition_file(self, partition_file_path):

        data_line = defaultdict(list)  # Almacena registros agrupados por transaction_id
        total_records = 0
        self.logger.debug(f"Leyendo la partición: {partition_file_path}")
        
        with open(partition_file_path, 'rb') as f:
            while True:
                try:
                    chunk = pickle.load(f)  # Carga un bloque
                    # Procesa cada registro del chunk directamente, sin acumular en "records"
                    for record in chunk:
                        transaction_id = record.get('transaction_id')
                        data_line[transaction_id].append(record)
                        total_records += 1
                    del chunk  # Libera memoria del chunk procesado
                except EOFError:
                    break
        
        self.logger.debug(f"Registros leídos: {total_records}")
        return data_line
    
    def get_partition_file_names(self, num_partitions: int) -> list:
        """
        Retorna una lista con los nombres de los archivos de partición,
        según el número de particiones dado.
        """
        partition_files = []
        for i in range(num_partitions):
            partition_file = f"{self.IncompleteTransactionsFile}_{i}.bin"
            partition_files.append(partition_file)

        return partition_files

    def process_transactions(self, data_line,partition_file):
          
        records_multisend = {}
        records_complete = []

        for transaction_id, records in data_line.items():
            flowctrl = False
            trx_in = False #indica si la transacción tiene un rastro de entrada (NEWTRANS O MNEWTRANS)
            trx_out = False #indica si la transacción tiene un rastro de salida (SEND)     
            result = {
                'Transaction ID': transaction_id,
                'Date Min': None,
                'date_max': None,
                'Priority': -1,
                'first_action': None,
                'first_subcomponent': None,
                'Last Action': None,
                'Last Subcomponent': None,
                'countSend': 0,
                'date_in_collector': None,
                'Duration': 0,
                'duration_limsp': 0,
                'NodeName': None,
                'Filename': None,
                'm_transaction_id': None
            }

            for record in records:
                timestamp = record['timestamp']
                action = record['action']
                subcomponent = record['subcomponent']
                priority = record.get('priority', -1)
                #mtransaction_id = record.get('mtransaction_id')
                
                if priority != -1:
                    result['Priority'] = priority

                '''if mtransaction_id is not None :
                    #Si hay un mtransaction el action relacionado es un "MNEWTRANS"
                    result['Transaction ID'] = mtransaction_id
                    result['m_transaction_id'] = transaction_id 
                    transaction_id = mtransaction_id
                    if not trx_in :
                        # esta condicion asegura que en los valores "min" tengan prioridad 
                        # los NEWTRANS, y solo se ponga el valor de MNEWTRANS cuando no haya un NEWTRANS 
                        result['Date Min'] = timestamp
                        result['first_action'] = action
                        result['first_subcomponent'] = subcomponent
                        incomplete_ok = True
                        trx_in = True
                    continue'''
                
                if action == 'NEWTRANS':
                    result['Date Min'] = timestamp
                    result['first_action'] = action
                    result['first_subcomponent'] = subcomponent
                    incomplete_ok = True
                    trx_in = True
                    continue
                      
                if action == 'SEND':
                    records_multisend[transaction_id] = [] 
                    if result['countSend'] == 0:                       
                        result['date_max'] = timestamp
                        result['Last Action'] = action
                        result['Last Subcomponent'] = subcomponent
                        result['countSend'] += 1
                        trx_out = True
                        incomplete_ok = True                                                             
                    else:
                        result['countSend'] +=1
                        records_multisend[transaction_id].append({
                            'Transaction ID': transaction_id,
                            'date_max': timestamp,
                            'Last Action': action,
                            'Last Subcomponent': subcomponent,
                        })                        
                    continue

                if not flowctrl:
                    if action == 'OUT' and subcomponent == 'FailOverManager' :
                        result['date_in_collector'] = timestamp
                        result['Last Action'] = action
                        result['Last Subcomponent'] = subcomponent
                        flowctrl = True 
                        incomplete_ok = True                       
                    continue
            
            #Si la transacción tiene un ciclo completo (entrada y salida) calcular duraciones y añadirlo a una lista para posteriormente escribirlo a disco
            if trx_in and trx_out :
                result['Duration'] = (result['date_max'] - result['Date Min']).total_seconds()
                if flowctrl :
                    result['duration_limsp'] = (result['date_in_collector'] - result['Date Min']).total_seconds()
                result['NodeName'] = records[0]['nodename']
                result['Filename'] = records[0]['filename']              
                records_complete.append(result.copy())                           
            '''elif incomplete_ok : 
                #Si la transacción no tiene un ciclo completo, pero tiene un NEWTRANS o un SEND se añade a la ventana, de lo contrario no se contempla
                self.records_incomplete[transaction_id] = copy.deepcopy(result)
                #self.records_incomplete.append(copy.deepcopy(result))'''              
                                
            result.clear()
            
            flowctrl = False 
            incomplete_ok = False
            trx_out = False
            trx_in = False

        return records_complete
    
    def process_transactions_deprecated(self, file_path,node_name,file_name):
        
        for detail in self.log_file_generator(file_path):
            transaction_id = detail['transaction_id']
            if transaction_id is None:
                self.logger.warning(f'Missing transaction_id in line from file: {file_path}. Details: {detail}')
                continue
            
            # Agregar los detalles al defaultdict bajo la clave `transaction_id`
            self.data_line[transaction_id].append({
                'timestamp': detail['timestamp'],
                'action': detail['action'],
                'subcomponent': detail['subcomponent'],
                'priority': detail.get('priority', -1),
                'mtransaction_id': detail.get('Mtransaction_id'),
                'nodename':node_name,
                'filename':file_name
            })
            self.total_lines += 1
        
        # Verificar si la cantidad de líneas en memoria es mayor al 90% del tamaño del chunk.
        # Dado que esta validación se hace al final del procesamiento del for (es decir de un archivo completo).
        # Al validar por encima del 80% se puede llegar a tener en memoria una cantidad de líneas superior al 90% e incluso
        # si un archivo fuera lo suficientemente grande un valor cercano o superior al 100%
        if self.total_lines >= self.chunk_size * 0.9:
            self.logger.debug(f"Total de líneas alcanzadas.:) {self.total_lines}. Se empiezan a procesar")
            self.process_records()
            self.data_line.clear()  # Liberar memoria  
            self.logger.debug(f"Se han terminado de procesar las {self.total_lines} transacciones y se inicia proceso de escritura a binario")        
            self.logger.debug(f"Dentro de este bloque se han dado por completadas con ayuda del slicing {self.count_complete_fromprevious}")
            self.logger.debug(f"registros completos {len(self.records_complete)}  - Registros incompletos {len(self.records_incomplete)}") 
     
            self.count_complete_fromprevious = 0
            self.total_lines = 0  # Reiniciar el contador

            self.write_in_threads()
                              
    def process_records_deprecated (self):
        records_multisend = {}
        flowctrl = False
        trx_in = False #indica si la transacción tiene un rastro de entrada (NEWTRANS O MNEWTRANS)
        trx_out = False #indica si la transacción tiene un rastro de salida (SEND)
        incomplete_ok = False
        self.process = True #Indica que se ha comenzado a procesar, este flag se utiliza para que el thread de escritura en logs no escriba mientras se está procesando

        if hasattr(self, 'thread_complete') and self.thread_complete.is_alive():
            self.thread_complete.join()
        if hasattr(self, 'thread_incomplete') and self.thread_incomplete.is_alive():
            self.thread_incomplete.join()
        
        for transaction_id, records in self.data_line.items(): 
            if transaction_id in self.records_incomplete:
                result = copy.deepcopy(self.records_incomplete[transaction_id])
                del self.records_incomplete[transaction_id]  # Elimina el registro de records_incomplete
                self.count_complete_fromprevious +=1                
                trx_in = True
                incomplete_ok = True
            else:
                result = {
                        'Transaction ID': transaction_id,
                        'Date Min': records[0]['timestamp'],
                        'date_max': records[0]['timestamp'],
                        'Priority': records[0]['priority'],
                        'first_action': records[0]['action'],
                        'first_subcomponent': records[0]['subcomponent'], 
                        'Last Action': records[0]['action'],
                        'Last Subcomponent': records[0]['subcomponent'],
                        'countSend': 0,
                        'date_in_collector': records[0]['timestamp'],
                        'Duration': 0,
                        'duration_limsp': 0,
                        'NodeName': records[0]['nodename'],                
                        'Filename': records[0]['filename'],
                        'm_transaction_id': None
                }
            
            for record in records:
                timestamp = record['timestamp']
                action = record['action']
                subcomponent = record['subcomponent']
                priority = record.get('priority', -1)
                mtransaction_id = record.get('mtransaction_id')

                if priority != -1:
                    result['Priority'] = priority

                if mtransaction_id is not None :
                    #Si hay un mtransaction el action relacionado es un "MNEWTRANS"
                    result['Transaction ID'] = mtransaction_id
                    result['m_transaction_id'] = transaction_id 
                    transaction_id = mtransaction_id
                    if not trx_in :
                        # esta condicion asegura que en los valores "min" tengan prioridad 
                        # los NEWTRANS, y solo se ponga el valor de MNEWTRANS cuando no haya un NEWTRANS 
                        result['Date Min'] = timestamp
                        result['first_action'] = action
                        result['first_subcomponent'] = subcomponent
                        incomplete_ok = True
                        trx_in = True
                    continue
                
                if action == 'NEWTRANS':
                    result['Date Min'] = timestamp
                    result['first_action'] = action
                    result['first_subcomponent'] = subcomponent
                    incomplete_ok = True
                    trx_in = True
                    continue
                      
                if action == 'SEND':
                    records_multisend[transaction_id] = [] 
                    if result['countSend'] == 0:                       
                        result['date_max'] = timestamp
                        result['Last Action'] = action
                        result['Last Subcomponent'] = subcomponent
                        result['countSend'] += 1
                        trx_out = True
                        incomplete_ok = True                                                             
                    else:
                        result['countSend'] +=1
                        records_multisend[transaction_id].append({
                            'Transaction ID': transaction_id,
                            'date_max': timestamp,
                            'Last Action': action,
                            'Last Subcomponent': subcomponent,
                        })
                        
                    continue
                if not flowctrl:
                    if action == 'OUT' and subcomponent == 'FailOverManager' :
                        result['date_in_collector'] = timestamp
                        result['Last Action'] = action
                        result['Last Subcomponent'] = subcomponent
                        flowctrl = True 
                        incomplete_ok = True                       
                    continue
            
            #Si la transacción tiene un ciclo completo (entrada y salida) calcular duraciones y añadirlo a una lista para posteriormente escribirlo a disco
            if trx_in and trx_out :
                result['Duration'] = (result['date_max'] - result['Date Min']).total_seconds()
                if flowctrl :
                    result['duration_limsp'] = (result['date_in_collector'] - result['Date Min']).total_seconds()
                self.records_complete.append(copy.deepcopy(result))                
            elif incomplete_ok : 
                #Si la transacción no tiene un ciclo completo, pero tiene un NEWTRANS o un SEND se añade a la ventana, de lo contrario no se contempla
                self.records_incomplete[transaction_id] = copy.deepcopy(result)
                #self.records_incomplete.append(copy.deepcopy(result))              
                
                
            result.clear()
            flowctrl = False 
            incomplete_ok = False
            trx_out = False
            trx_in = False

        self.process = False
   
    def write_in_threads(self, processed_data):         
        self.thread_complete = threading.Thread(target=self.write_result_to_csv, args=(processed_data,),daemon=True)
        #self.thread_incomplete = threading.Thread(target=self.write_incomplete_to_binary, daemon=True)

        # Iniciar los hilos
        self.thread_complete.start()
        #self.thread_incomplete.start()

    def write_incomplete_to_binary_deprecated(self,final=False):
        try:
            if not self.records_incomplete:  # Verifica si self.records_incomplete está vacío
                self.logger.debug('No hay transacciones incompletas para escribir.')
                return
            
            self.logger.debug(f"Se inicia escritura de transacciones incompletas al archivo binario")
            # Calcular el número máximo de registros a conservar
            max_to_keep = int(self.chunk_size * 0.1)
            
            # Obtener el excedente de registros
            exceeding_count = len(self.records_incomplete) - max_to_keep
            
            if final:
                exceeding_count = len(self.records_incomplete)

            if exceeding_count > 0:
                # Extraer los primeros 'exceeding_count' registros como una lista
                exceeding_records = list(self.records_incomplete.values())[:exceeding_count]
                
                # Actualizar 'self.records_incomplet' para conservar solo los últimos `max_to_keep` registros
                self.records_incomplete = {
                    k: v for i, (k, v) in enumerate(self.records_incomplete.items())
                    if i >= exceeding_count
                }
                
                # Escribir los registros excedentes en el archivo binario
                with open(self.IncompleteTransactionsFile, 'ab') as bin_file:
                    pickle.dump(exceeding_records, bin_file)
                
                self.logger.info(f"{len(exceeding_records)} transacciones incompletas escritas en el archivo binario {self.IncompleteTransactionsFile}")
                self.logger.info(f"Se mantienen {len(self.records_incomplete)} trx incompletas ")
                self.count_incomplete_write += len(exceeding_records)
                exceeding_records.clear()
            else:
                self.logger.info("No hay registros excedentes para escribir en el archivo binario.")
        except Exception as e:
            self.logger.error(f"Error al escribir las transacciones incompletas al archivo binario: {e}")
        finally:
            gc.collect()
            # se fuerza la invocación del GC de Python ya que dentro de write_incomplete_to_binary se libera  
            # memoria del diccionario records_incomplete, básicamente son los registros que se escriben al archivo binario los que se eliminan

    def write_result_to_binary_deprecated(self,records_complete):
        
        try:
            if not records_complete:  # Verifica si self.records_complete está vacío
                self.logger.debug('No hay transacciones completas para escribir.')
                return
            
            self.logger.debug(f"Se inicia escritura de transacciones completas al archivo binario")
            with self.lock:  # Bloquea el recurso compartido
                with open(self.CompletedTransactionsFile, 'ab') as bin_file:  # 'ab' para agregar datos en formato binario
                    pickle.dump(records_complete, bin_file)
                self.logger.info(f"{len(records_complete)} Transacciones completadas {self.CompletedTransactionsFile}")
                self.count_trx_complete += len(records_complete)

        except Exception as e:
            self.logger.error(f"Error al escribir transacciones completadas al archivo: {e}")


    def write_result_to_csv(self, records_complete):
        try:
            if not records_complete:
                self.logger.debug('No hay transacciones completas para escribir.')
                return
            
            self.logger.debug(f"Se inicia escritura de transacciones al archivo binario. {len(records_complete)}")
            with self.csv_lock:  # Bloqueo para evitar condiciones de carrera
                # Abrir archivo en modo append
                with open(self.resultFinalFile, 'a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=[
                        'Transaction ID', 'Date Min', 'date_max', 'Priority',
                        'first_action', 'first_subcomponent', 'Last Action', 
                        'Last Subcomponent', 'countSend', 'date_in_collector', 
                        'Duration', 'duration_limsp', 'NodeName', 'Filename', 
                        'm_transaction_id'
                    ])

                    # Escribir encabezado solo una vez
                    if not self.csv_initialized:
                        writer.writeheader()
                        self.csv_initialized = True

                    # Escribir los registros en bloques
                    writer.writerows(records_complete)

                self.logger.info(f"{len(records_complete)} transacciones escritas en CSV")
                self.count_trx_complete += len(records_complete)

        except Exception as e:
            self.logger.error(f"Error al escribir CSV: {e}")

    def log_file_generator(self, file_path):
        with open(file_path, 'r') as file: 
            for line in file:
                processed_line = self.process_log_line(line)
                if processed_line:
                    yield processed_line

    def process_log_line(self,line):
        """
        Esta función procesa una línea de log y extrae los detalles relevantes.
        """
        
        transaction_pattern = r"(transaction:)([^ ]*)"
        priority_pattern = r"pri:(\d+)"
        match = self.pattern.match(line)

        if match:
            details = match.groupdict()
            action = details['action']
            subcomponent = details['subcomponent']
            if action not in self.valid_actions :
                if subcomponent not in self.valid_subcomponents :                
                    return None                
                elif action != 'OUT' :
                        return None  

            # Filtrar si el action no está en valid_actions y el subcomponent no está en valid_subcomponents
            '''if action not in self.valid_actions and  subcomponent not in self.valid_subcomponents:
                return None
            
            if subcomponent not in self.valid_subcomponents and action != 'OUT' :
                if action not in self.valid_actions :
                    return None'''

            transaction_matches = re.finditer(transaction_pattern, details['details'])
            transaction_ids = []
            for transaction_match in transaction_matches:
                transaction_id = transaction_match.group(2)
                transaction_ids.append(transaction_id)

            if len(transaction_ids) >= 1:
                details['transaction_id'] = transaction_ids[0]
            else:
                details['transaction_id'] = None
                return None

            if len(transaction_ids) >= 2:
                details['Mtransaction_id'] = transaction_ids[1]
            else:
                details['Mtransaction_id'] = None            

            priority_match = re.search(priority_pattern, details['details'])
            if priority_match:
                details['priority'] = int(priority_match.group(1))
            else:
                details['priority'] = -1

            if '.' in details['timestamp']:
                details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
            else:
                details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S")

            return details

        return None

    def write_dataconfig(self):
        self.logger.info("VERSION 4.4")
        self.logger.info(f"inputPath: {self.inputFile}")
        self.logger.info(f"filePattern: {self.filePattern}")
        self.logger.info(f"IncompleteTransactionsFile: {self.IncompleteTransactionsFile}")
        self.logger.info(f"CompleteTransactionsFile: {self.CompletedTransactionsFile}")
        self.logger.info(f"chunk_size: {self.chunk_size}") 
        self.logger.info(f"timeToLog: {self.timeToLog}")
      
if __name__ == "__main__":
    
    num_partitions = 30  # O el valor que estés utilizando
    processed_data =[] # Lista para almacenar los resultados finales

    try:
        manager = ProcessorFiles('./config/config.ini')        
        logging.info("Iniciando proceso.. ")
        
        manager.compile_regular_expression()
        manager.write_dataconfig()
        manager.process_log_files()
            
        partition_file_names = manager.get_partition_file_names(num_partitions)
        for partition_file in partition_file_names:
            data_line = manager.process_partition_file(partition_file)
            processed_data = manager.process_transactions(data_line, partition_file)
            manager.logger.debug(f"Partición {partition_file} procesada. Inicia proceso de escritura")
            manager.write_result_to_csv(processed_data)
            #manager.write_in_threads(processed_data)       
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        end_time = time.time()
        total_time = end_time - manager.start_time
        logging.info(f"Se escriben {manager.count_trx_complete} transacciones completas")
        logging.info(f"Se descartan {manager.count_incomplete_write} transacciones")
        if total_time > 3600 :
            logging.info(f"Tiempo total: {total_time / 3600:.2f} horas.")
        else:
            logging.info(f"Tiempo total: {total_time / 60 :.2f} minutos.")