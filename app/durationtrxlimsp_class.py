import os
import configparser
import logging
import re
import sys
import pickle
import threading
from datetime import datetime
import time
from pathlib import Path
from collections import defaultdict
import copy

class ProcessorFiles:
    def __init__(self, config_file):

        self.config = self.load_config(config_file)

        if 'LOGGING' not in self.config:
            raise KeyError("'LOGGING' section not found in the configuration file.")
        
        self.setup_logging()    
        self.lines_count = 0  # Contador para el número de registros en self.data_line
        self.pattern = None
        self.valid_actions = set()
        self.valid_subcomponents = set()
        self.node_name = str()
        self.file_name = str()
        self.countFiles = 0 #contador para la cantidad de archivos procesados (para efectos de log y progreso)
        self.totalFiles = 0 #Total de archivos a procesar, principalmente para efectos de log
        self.count_incomplete = 0
        self.count_trx_complete = 0 
        self.write_thread = None
        self.write_thread_complete = None
        #self.data_line = []
        self.data_line = defaultdict(list)
        self.count_incomplete_write = 0 # contador para conocer cuantos registros se han escrito al binario de descartadas  
        self.results = [] # Lista para almacenar los resultados finales
        self.results_inprogress = [] # Lista para almacenar las transacciones que aun no son finales pero que podrian tener un estado final en otro bloque de procesamiento.
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
        self.filePattern = self.config['PROCESS_FILES']['FilePattern']
        self.IncompleteTransactionsFile = self.config['PROCESS_FILES']['IncompleteTransactionsFile']
        self.CompletedTransactionsFile = self.config['PROCESS_FILES']['CompletedTransactionsFile']
        #self.archivoResultante = self.config['PROCESS_FILES']['OutputFilePath']
        self.chunk_size = self.config['PROCESS_FILES'].getint('Chunk_size_write', 1000000)
        
        self.mem_trx_security = self.config['PROCESS_FILES'].getint('mem_trx_security', 5000000)
        self.discarded = self.config['PROCESS_FILES'].getboolean('writeDiscarded', fallback=False)
        self.valid_actions = set(self.config['PROCESS_FILES']['valid_actions'].split(','))
        self.valid_subcomponents = set(self.config['PROCESS_FILES']['valid_subcomponents'].split(','))
        self.timeToLog = self.config['PROCESS_FILES'].getint('timeToLog', fallback=60)

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
        # Recorrer todos los archivos en el directorio, incluidos subdirectorios
        files = [file for file in Path(self.inputFile).rglob(self.filePattern) if file.is_file()]        
        # Ordenar los archivos por la fecha de modificación (de más antigua a más reciente)
        files_sorted = sorted(files, key=lambda file: file.stat().st_mtime)        
        return files_sorted        

    def process_log_files(self):   
        
        self.directory_path = Path(self.inputFile)
        
        # Verificar si los archivos existen y eliminarlos        
        if os.path.exists(self.IncompleteTransactionsFile):
            os.remove(self.IncompleteTransactionsFile)        
        if os.path.exists(self.CompletedTransactionsFile):
            os.remove(self.CompletedTransactionsFile)
        
        matching_files = list(self.directory_path.rglob(self.filePattern)) 
        self.totalFiles = len(matching_files)

        if not matching_files:
            logging.error(f'No files found. Terminating the script. {self.directory_path} archivos {self.filePattern}')
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
                self.process_transactions(file_path,node_name,file_name)
            except Exception as e:
                self.loggerfiles.error(f'Error processing file {file_name}: {e}')
        
        
        # Escribir los datos restantes al final del archivo
        if self.data_line:
            self.prepare_transactions()
            self.data_line.clear()
            self.write_result_to_binary()

        if self.results_inprogress :
            #atencion, se esta duplicando el tamanio en memoria de los incompletos, ya esta en una variable self, sobra esta asignacion
            exceeding_records = self.results_inprogress
            self.write_exceeding_records_to_binary(exceeding_records)
            
        self.keep_running = False
        progress_thread.join()

  
    def log_progress(self):
        while self.keep_running:
            parcial_time = time.time()
            total_time = parcial_time - self.start_time
            progressFiles = (self.countFiles/self.totalFiles)*100
            self.logger.info(f"Total de archivos procesados {self.countFiles}, progreso {progressFiles:.2f}%")
            if total_time > 3600 :
                logging.info(f"Tiempo transcurrido: {total_time / 3600:.2f} horas.")
            else:
                logging.info(f"Tiempo transcurrido: {total_time / 60:.2f} minutos.")
            time.sleep(self.timeToLog)  # Esperar x segundos                               

        
    def process_transactions(self, file_path,node_name,file_name):
        
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
            self.lines_count += 1

            # Verificar si el contador alcanza el tamaño del chunk
            if self.lines_count >= self.chunk_size:
                self.prepare_transactions()
                self.data_line.clear()  # Liberar memoria
                self.lines_count = 0  # Reiniciar el contador
                if self.write_thread_complete and self.write_thread_complete.is_alive():
                    self.write_thread_complete.join()

                self.write_thread_complete = threading.Thread(target=self.write_result_to_binary)
                self.write_thread_complete.start()
                #self.write_result_to_binary()
                              

    def prepare_transactions (self):
        
        self.logger.info(f"se procesaran {len(self.data_line)} transacciones correspondientes a {self.lines_count} líneas")
  
        records_multisend = {}
        flowctrl = False
        trx_in = False #indica si la transacción tiene un rastro de entrada (NEWTRANS O MNEWTRANS)
        trx_out = False #indica si la transacción tiene un rastro de salida (SEND)
        
        incomplete_ok = False
       
        if self.count_incomplete > self.mem_trx_security :            
            incomplete = self.count_incomplete - self.mem_trx_security
            self.logger.debug(f"Se añadiran {self.mem_trx_security} transacciones del bloque anterior y se escribiran como incompletas {incomplete}")
            if self.write_thread and self.write_thread.is_alive():                
                self.write_thread.join()  # Espera a que el hilo termine

            exceeding_records = self.results_inprogress[self.mem_trx_security:]
            self.write_thread = threading.Thread(target=self.write_exceeding_records_to_binary, args=(exceeding_records,))            
            self.write_thread.start()
        else :
             self.logger.debug(f"Se añadiran {self.count_incomplete} transacciones del bloque anterior")   


        # Inicializar records_previous como diccionario que contendrá la agrupación de transacciones
        records_previous = {transaction['Transaction ID']: transaction for transaction in self.results_inprogress[:self.mem_trx_security]}
        #records_previous = {transaction['Transaction ID']: transaction for transaction in self.results_inprogress}
        self.results_inprogress.clear()
        self.count_incomplete = 0

        for transaction_id, records in self.data_line.items():
            if transaction_id in records_previous:
                result = records_previous[transaction_id]
                trx_in = True
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
                        'Filename': records[0]['filename']
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
                    result['Transaction ID'] = mtransaction_id
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
                #comento este bloque, en principio si hay un MNEWTRANS es porque se ha desdoblado lo cual 
                # se valida en el if mtransaction_id is not None mas arriba asi que se pondra parte de esta
                # logica arriba .

                '''elif action == 'MNEWTRANS' and not trx_in : 
                    # esta condicion independiente para MNEWTRANS asegura que en los valores "min" tengan prioridad 
                    # los NEWTRANS, y solo se ponga el valor de MNEWTRANS cuando no haya un NEWTRANS 
                    result['Date Min'] = timestamp
                    result['first_action'] = action
                    result['first_subcomponent'] = subcomponent
                    incomplete_ok = True
                    trx_in = True
                    continue'''                
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
                    continue

            #Si la transacción tiene un ciclo completo (entrada y salida) calcular duraciones y añadirlo a una lista para posteriormente escribirlo a disco
            if trx_in and trx_out :
                result['Duration'] = (result['date_max'] - result['Date Min']).total_seconds()       
                      
                if flowctrl :
                    result['duration_limsp'] = (result['date_in_collector'] - result['Date Min']).total_seconds()
                self.results.append(copy.deepcopy(result))
            elif incomplete_ok : 
                #Si la transacción no tiene un ciclo completo, pero tiene un NEWTRANS o un SEND se añade a la ventana, de lo contrario no se contempla
                self.results_inprogress.append(copy.deepcopy(result))
                records_previous[transaction_id] = copy.deepcopy(result)
                #records_previous[transaction_id] = result 
                self.count_incomplete +=1    
                          
            result.clear()
            flowctrl = False 
            incomplete_ok = False
            trx_out = False
            trx_in = False
        #print(f"cantidad de SEND {count_sendbrr}")
        records_previous.clear()
        self.logger.debug(f"se ha procesado {len(self.results)} transacciones completas")
   
    def write_exceeding_records_to_binary(self, exceeding_records):
       
        try:
            with open(self.IncompleteTransactionsFile, 'ab') as bin_file:  # 'ab' para agregar datos en formato binario
                pickle.dump(exceeding_records, bin_file)
            self.logger.info(f"{len(exceeding_records)} transacciones incompletas escritas en el archivo binario {self.IncompleteTransactionsFile}")
            self.count_incomplete_write += len(exceeding_records)
            
        except Exception as e:
            self.logger.error(f"Error al escribir las transacciones incompletas al archivo binario: {e}")

    def write_result_to_binary(self):
            try:
                with open(self.CompletedTransactionsFile, 'ab') as bin_file:  # 'ab' para agregar datos en formato binario
                    pickle.dump(self.results, bin_file)
                self.logger.info(f"{len(self.results)} Transacciones completadas {self.CompletedTransactionsFile}")
                self.count_trx_complete += len(self.results)
                self.results.clear()
            except Exception as e:
                self.logger.error(f"Error al escribir transacciones completadas al archivo: {e}")

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
            # Filtrar si el action no está en valid_actions y el subcomponent no está en valid_subcomponents
            if action not in self.valid_actions and subcomponent not in self.valid_subcomponents:
                return None

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
        self.logger.info("VERSION 2.5")
        self.logger.info(f"inputPath: {self.inputFile}")
        self.logger.info(f"filePattern: {self.filePattern}")
        self.logger.info(f"IncompleteTransactionsFile: {self.IncompleteTransactionsFile}")
        self.logger.info(f"CompleteTransactionsFile: {self.CompletedTransactionsFile}")
        self.logger.info(f"chunk_size: {self.chunk_size}")
        self.logger.info(f"mem_trx_security: {self.mem_trx_security}")
        self.logger.info(f"discarded: {self.discarded}")
        self.logger.info(f"timeToLog: {self.timeToLog}")
        


if __name__ == "__main__":
    

    try:
        manager = ProcessorFiles('./config/config.ini')
        
        logging.info("Iniciando proceso.. ")
        # Ejemplo de uso del método write_dataconfig
        manager.compile_regular_expression()
        manager.write_dataconfig()
        manager.process_log_files()

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