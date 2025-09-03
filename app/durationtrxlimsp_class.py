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
        #self.mem_trx_security = self.config['PROCESS_FILES'].getint('mem_trx_security', 5000000)        
        self.valid_actions = set(self.config['PROCESS_FILES']['valid_actions'].split(','))
        self.valid_subcomponents = set(self.config['PROCESS_FILES']['valid_subcomponents'].split(','))
        # tienmpo en segundos que se ejecutará el hilo que registra actividad en los logs
        self.timeToLog = self.config['PROCESS_FILES'].getint('timeToLog', fallback=120)
        self.resultFinalFile = self.config['PROCESS_FILES']['ResultFinalFile']
        self.order_patterns = self.config['PROCESS_FILES'].get('file_order_patterns', 'limsp_adaptor*,limmsp_bus_massive*,limsp_bus*,limsp_collector*').split(',')
        self.num_partitions = self.config['PROCESS_FILES'].getint('num_partitions', 30)  # 30 como valor por defecto

        # Cargar el patrón desde la configuración
        pattern = self.config['PROCESS_FILES'].get('log_pattern')
        if pattern:
            # Si existe en config, eliminar comillas dobles y procesar escapes
            pattern = pattern.strip('"')
            # Convertir los dobles backslashes en singles
            pattern = pattern.replace('\\\\', '\\')
        
        # Usar el patrón procesado o el valor por defecto
        self.log_pattern_str = pattern or r'\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)'

    def compile_regular_expression(self):
        try:
            self.pattern = re.compile(self.log_pattern_str)
            self.logger.debug(f"Expresión regular compilada: {self.log_pattern_str}")
        except re.error as e:
            self.logger.error(f"Error compilando expresión regular: {str(e)}")
            raise

    def orderbydate(self):
        """
        Ordena los archivos según un orden de patrones configurable y luego por fecha de modificación.
        """
        files = [file for file in Path(self.inputFile).rglob(self.filePattern) if file.is_file()]        
        
        # Obtener el orden de los patrones desde la configuración        
        order_patterns = [pattern.strip() for pattern in self.order_patterns]
        
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
        
        # Verificar si los archivos existen y eliminarlos        
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
            path, file_name = os.path.split(file_path)
            self.loggerfiles.debug(f'Nodo: {node_name} -- Archivo ({self.countFiles}): {file_name} -- Path: {path}')   
            try:
                self.create_partitions(file_path, node_name, file_name, self.num_partitions)
            except Exception as e:
                # Logging detallado del error
                
                error_details = traceback.format_exc()
                self.loggerfiles.error(
                    f'Error processing file {file_name}:\n'
                    f'Error type: {type(e).__name__}\n'
                    f'Error message: {str(e)}\n'
                    f'Stack trace:\n{error_details}'
                )
                # Información adicional del estado
                self.loggerfiles.error(
                    f'Estado actual:\n'
                    f'- Líneas procesadas: {self.total_lines}\n'
                    f'- Registros FailOver pendientes: {len(self.first_fo_records)}\n'
                    f'- Tamaño buffer particiones: {sum(len(buf) for buf in self.partition_buffers.values())}'
                )
        
        # Escribir los datos restantes al final del archivo
        self.logger.info('Se han procesado todos los archivos. Se escriben los registros restantes...')
        if self.first_fo_records:
            self.logger.info(f"{len(self.first_fo_records)} registros de FailOver quedaron sin relacionar")

        if any(self.partition_buffers.values()):
            self.flush_partition_buffers(self.num_partitions)
            
        self.logger.info('Se han creado todas las particiones. Se inicia el procesamiento de estas...')
        del self.uf  # Liberar memoria de la estructura Union-Find
        del self.partition_buffers  # Liberar memoria de los buffers de particiones
        del self.first_fo_records  # Liberar memoria de la lista de registros de FailOver
        gc.collect()  # Forzar la recolección de basura

        self.keep_running = False
        progress_thread.join()
  
    def log_progress(self):
        while self.keep_running:
            #self.monitor_resources()  # Añadir monitoreo
            if not self.process :
                parcial_time = time.time()
                total_time = parcial_time - self.start_time
                progressFiles = (self.countFiles/self.totalFiles)*100
                total_fo_records = len(self.first_fo_records)
                self.logger.info(f"Total de archivos procesados {self.countFiles}, progreso {progressFiles:.2f}%. {total_fo_records} registros de FailOver sin relacionar")
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

    def create_partitions(self, file_path, node_name, file_name, num_partitions: int):
        '''import psutil
        process = psutil.Process()'''
        
        try:
            #initial_memory = process.memory_info().rss / 1024 / 1024  # MB
            #self.loggerfiles.debug(f'Memoria inicial: {initial_memory:.2f} MB')
            
            if not hasattr(self, 'partition_buffers'):
                self.partition_buffers = {i: [] for i in range(num_partitions)}
                        
            detail_fo = []
            
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

                if action == 'OUT' and subcomponent == 'FailOverManager' :                    
                    if  self.uf.get_tree_size(transaction_id) == 0:
                        '''Esto indica que el FailOver es el primer registro de la transacción, por lo tanto no se
                        calculará aún el hash y se almacenará en el diccionario global usando transaction_id como clave'''                        
                        self.first_fo_records[transaction_id] = detail 
                        continue
                
                self.uf.add_transaction(transaction_id)
                
                mtransaction_id = detail.get('Mtransaction_id')
                if mtransaction_id is not None:
                    self.uf.add_transaction(mtransaction_id)
                    self.uf.union(transaction_id, mtransaction_id)
                    if mtransaction_id in self.first_fo_records:
 
                        detail_fo = self.first_fo_records.pop(mtransaction_id)
                        canonical_id = self.uf.find(mtransaction_id)
                        if canonical_id is None:
                            canonical_id = transaction_id
                        detail_fo['transaction_id'] = canonical_id
                        self.total_lines += 1
                        detail_fo['nodename'] = node_name
                        detail_fo['filename'] = file_name
                        partition_index = self.get_partition(canonical_id, num_partitions)
                        self.partition_buffers[partition_index].append(detail_fo)

                elif action == 'SEND':                   
                    if transaction_id in self.first_fo_records:                        
                        # Si el transaction_id está en first_fo_records y es un un SEND, significa que hay un FailOverManager
                        # asociado a este SEND que no se ha vinculado a un MNewtrans.
                        detail_fo = self.first_fo_records.pop(transaction_id)
                        canonical_id = self.uf.find(transaction_id)
                        if canonical_id is None:
                            canonical_id = transaction_id
                        detail_fo['transaction_id'] = canonical_id
                        detail_fo['nodename'] = node_name
                        detail_fo['filename'] = file_name
                        self.total_lines += 1
                        partition_index = self.get_partition(canonical_id, num_partitions)
                        self.partition_buffers[partition_index].append(detail_fo)
                    elif self.uf.get_tree_size(transaction_id) == 1:                        
                        '''Esto indica que el SEND es el primer registro de la transacción, por lo tanto no se
                        calculará aún el hash y se almacenará en el diccionario global usando transaction_id como clave'''                        
                        self.first_fo_records[transaction_id] = detail 
                        continue


                canonical_id = self.uf.find(transaction_id)   
                if canonical_id is None:
                    canonical_id = transaction_id             
                detail['transaction_id'] = canonical_id                
                detail['nodename'] = node_name
                detail['filename'] = file_name
                self.total_lines += 1
                
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
            #final_memory = process.memory_info().rss / 1024 / 1024
            '''self.loggerfiles.debug(
                f'Memoria final: {final_memory:.2f} MB\n'
                f'Diferencia: {final_memory - initial_memory:.2f} MB'
            )'''
        except Exception as e:
            #current_memory = process.memory_info().rss / 1024 / 1024
            self.loggerfiles.error(
                f'Error en UnionFind operations:\n'
                f'Transaction ID: {transaction_id}\n'
                #f'Error con uso de memoria: {current_memory:.2f} MB\n'
                f'Error: {str(e)}'
            )
            raise
   
               
    def process_partition_file(self, partition_file_path):
        """
        Procesa un archivo de partición y retorna los registros agrupados por transaction_id.
        
        Args:
            partition_file_path (str): Ruta al archivo de partición
        
        Returns:
            defaultdict: Diccionario con registros agrupados por transaction_id
            None: Si el archivo no existe o hay error en la lectura
        """
        data_line = defaultdict(list)
        total_records = 0

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
                            transaction_id = record.get('transaction_id')
                            if transaction_id:  # Validación adicional
                                data_line[transaction_id].append(record)
                                total_records += 1
                        del chunk  # Libera memoria
                    except EOFError:
                        break
                    except pickle.UnpicklingError as e:
                        self.logger.error(f"Error al deserializar datos en {partition_file_path}: {str(e)}")
                        break

            self.logger.debug(f"Registros leídos: {total_records}")
            return data_line

        except IOError as e:
            self.logger.error(f"Error de I/O al leer {partition_file_path}: {str(e)}")
            return data_line
        except Exception as e:
            self.logger.error(f"Error inesperado procesando {partition_file_path}: {str(e)}")
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

    def process_transactions(self, data_line):
          
        # El defaultdict externo crea un defaultdict(dict) cuando una clave no existe.
        records_multisend = defaultdict(lambda: defaultdict(dict))
        
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
                'countMNewtrans':0,
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
                node_name = record.get('nodename')
                file_name = record.get('filename')
                mtransaction_id = record.get('Mtransaction_id')

                if mtransaction_id :
                    result['countMNewtrans'] +=1
                    if result['countMNewtrans'] > 1:
                        records_multisend[transaction_id][mtransaction_id].update({
                            'm_transaction_id': mtransaction_id
                        })
                    else:
                        result['m_transaction_id'] = mtransaction_id
                
                else:
                    pattern_mtrx = r"transaction:(\S+)"
                    match_mtrx = re.search(pattern_mtrx, record.get('details'))
                    if match_mtrx:
                        # Si se encontró el patrón, el grupo 1 contiene nuestro ID
                        mtransaction_id = match_mtrx.group(1)
                    else:
                        mtransaction_id = None
               
                if priority != -1:
                    result['Priority'] = priority
                
                if action == 'NEWTRANS':
                    result['Date Min'] = timestamp
                    result['first_action'] = action
                    result['first_subcomponent'] = subcomponent
                    result['NodeName']= node_name
                    result['Filename'] = file_name      
                    trx_in = True
                    continue
                      
                if action == 'SEND':                    
                    if mtransaction_id == result['m_transaction_id']:                                                                      
                        result['date_max'] = timestamp
                        result['Last Action'] = action
                        result['Last Subcomponent'] = subcomponent
                        result['countSend'] += 1
                        trx_out = True                                                                                     
                    else:
                        result['countSend'] +=1
                        # Accedemos al registro específico usando ambos IDs y lo actualizamos
                        records_multisend[transaction_id][mtransaction_id].update({
                            'Transaction ID': transaction_id,
                            'date_max': timestamp,
                            'Last Action': action,
                            'Last Subcomponent': subcomponent
                        })
                                                                    
                    continue

                if action == 'OUT' and subcomponent == 'FailOverManager' :
                    flowctrl = True
                    if mtransaction_id == result['m_transaction_id'] or result['countMNewtrans'] == 0:                                       
                        result['date_in_collector'] = timestamp
                    else:
                         records_multisend[transaction_id][mtransaction_id].update({
                            'date_in_collector': timestamp
                        })                                                       
                    continue

            if transaction_id in records_multisend:
                # Obtenemos una referencia al diccionario interno para trabajar con él.
                m_records_dict = records_multisend[transaction_id]
                # Lista para recolectar los sub-registros que no cumplan la condición
                m_ids_to_delete = []

                # 2. Iteramos sobre los sub-registros para encontrar los que no tienen 'date_max'.
                #    Usamos .items() para obtener tanto la clave (m_id) como el diccionario de datos.
                for m_id, record_data in m_records_dict.items():
                    if 'date_max' not in record_data:
                        # 3. "Marcamos" el sub-registro para su eliminación.
                        m_ids_to_delete.append(m_id)

                 # 4. Una vez terminado el bucle, eliminamos de forma segura los registros marcados.
                if m_ids_to_delete:                    
                    for m_id in m_ids_to_delete:
                        del m_records_dict[m_id]
            
            #Si la transacción tiene un ciclo completo (entrada y salida) calcular duraciones y añadirlo a una lista para posteriormente escribirlo a disco
            if trx_in and trx_out :
                result['Duration'] = (result['date_max'] - result['Date Min']).total_seconds()
                if flowctrl :
                    result['duration_limsp'] = (result['date_in_collector'] - result['Date Min']).total_seconds()
                #result['NodeName'] = records[0]['nodename']
                #result['Filename'] = records[0]['filename']              
                records_complete.append(result.copy())      
            elif trx_in :
                #Si la transacción termina solo con NEWTRANS se incluye bajo las siguientes convenciones
                result['Last Action'] = 'KO'
                result['Last Subcomponent'] = 'KO'
                result['countSend'] = 0
                result['date_in_collector'] = None
                result['Duration'] = 0
                result['duration_limsp'] = 0
                result['date_max'] = result['Date Min']
                records_complete.append(result.copy())
                                
            result.clear()            
            flowctrl = False            
            trx_out = False
            trx_in = False

        if records_multisend:
            trx_data = {
                record['Transaction ID']: {
                    'Date Min': record['Date Min']
                }
                for record in records_complete
            }
            for transaction_id, m_records_dict in records_multisend.items():
                if transaction_id in trx_data:
                    date_min = trx_data[transaction_id]['Date Min']
                    for record_data in m_records_dict.values():
                        try:
                            # Convertir date_max a datetime si no lo está ya
                            if isinstance(record_data.get('date_max'), str):
                                record_data['date_max'] = datetime.strptime(record_data['date_max'], "%Y/%m/%d %H:%M:%S.%f")
                            
                            if isinstance(record_data.get('date_in_collector'),str):
                               record_data['date_in_collector'] = datetime.strptime(record_data['date_in_collector'], "%Y/%m/%d %H:%M:%S.%f") 
                            
                            # Asegurarnos que tenemos un date_max válido antes de calcular
                            if record_data.get('date_max'):
                                # Calcular duration como la diferencia entre date_max y Date Min
                                duration = (record_data['date_max'] - date_min).total_seconds()
                                record_data['Duration'] = duration
                            else:
                                # Opcional: manejar el caso donde una fila no tiene 'date_max'
                                record_data['Duration'] = 0 

                            if record_data.get('date_in_collector'):
                                # Calcular duration_limsp como la diferencia entre date_in_collector y Date Min
                                duration = (record_data['date_in_collector'] - date_min).total_seconds()
                                record_data['duration_limsp'] = duration
                            else:
                                # Opcional: manejar el caso donde una fila no tiene 'date_max'
                                record_data['duration_limsp'] = 0 
                        
                        except (TypeError, ValueError) as e:
                            # Opcional pero recomendado: Manejar errores si las fechas no son válidas
                            # print(f"No se pudo calcular la duración para el registro {record_data.get('m_transaction_id')}: {e}")
                            record_data['duration'] = 0
                            record_data['duration_limsp'] = 0
                else:
                    self.logger.warning(f"Transaction ID {transaction_id} not found in trx_data. Skipping multisend record.")
                    continue

        return records_complete, records_multisend
    
    def write_in_threads(self, processed_data, records_multisend):
        """
        Inicia hilos para escribir datos procesados y registros multisend.
        La validación de processed_data se hace antes de llamar a este método.
        
        Args:
            processed_data (list): Lista de transacciones procesadas
            records_multisend (dict): Diccionario de registros multisend
        """
        threads = []
        
        # Crear hilo para datos procesados (sin validación)
        thread_complete = threading.Thread(
            target=self.write_result_to_csv,
            args=(processed_data,),
            daemon=True
        )
        threads.append(thread_complete)

        # Crear hilo para multisend solo si hay datos
        if records_multisend:
            thread_multisend = threading.Thread(
                target=self.write_multisend_to_csv,
                args=(records_multisend,),
                daemon=True
            )
            threads.append(thread_multisend)

        # Iniciar todos los hilos
        for thread in threads:
            thread.start()

        # Esperar a que todos los hilos terminen
        for thread in threads:
            thread.join()

    def write_result_to_csv(self, records_complete):
        try:
            if not records_complete:
                self.logger.debug('No hay transacciones completas para escribir.')
                return
            
            self.logger.debug(f"Se inicia escritura de transacciones al archivo binario. {len(records_complete)}")
            fieldnames = [
            'Transaction ID', 'Date Min', 'date_max', 'Priority',
            'first_action', 'first_subcomponent', 'Last Action', 
            'Last Subcomponent', 'countSend', 'date_in_collector', 
            'Duration', 'duration_limsp', 'NodeName', 'Filename', 
            'm_transaction_id'
            ]
            
            with self.csv_lock:  # Bloqueo para evitar condiciones de carrera
                # Abrir archivo en modo append
                with open(self.resultFinalFile, 'a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')

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

    def write_multisend_to_csv(self, records_multisend):
        """
        Escribe los registros de multisend en un archivo CSV separado.
        
        Args:
            records_multisend (dict): Diccionario con registros de envíos múltiples
        """
        try:
            if not records_multisend:
                self.logger.debug('No hay transacciones multisend para escribir.')
                return
                
            multisend_file = self.resultFinalFile.replace('.csv', '_multisend.csv')
            self.logger.debug(f"Se inicia escritura de transacciones multisend. Total IDs: {len(records_multisend)}")

            csv_headers = [
                'Transaction ID', 'Date Min', 'date_max', 'Priority', 'first_action', 
                'first_subcomponent', 'Last Action', 'Last Subcomponent', 'countSend', 
                'date_in_collector', 'Duration', 'duration_limsp', 'NodeName', 
                'Filename', 'm_transaction_id'
            ]
            # Usamos un contador para llevar la cuenta de las filas reales escritas
            rows_written = 0

            with self.csv_lock:                
                with open(multisend_file, 'a', newline='', encoding='utf-8') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=csv_headers)

                    # Escribir encabezado si el archivo es nuevo/está vacío
                    if csv_file.tell() == 0:
                        writer.writeheader()

                    # 2. Iteramos sobre la estructura anidada para escribir los datos
                    # Bucle Externo: no cambia
                    for transaction_id, m_records_dict in records_multisend.items():
                        
                        # Bucle Interno: usamos .items() para obtener la clave y el valor
                        # 'm_id' será la clave (ej: 'UNO51...oY4')
                        # 'record_data' será el diccionario con los datos de la fila
                        for m_id, record_data in m_records_dict.items():                            
                            # 3. El Truco Clave: Añadimos el m_transaction_id al diccionario
                            #    justo antes de escribirlo. DictWriter ahora encontrará este campo.
                            #    Esto cumple tu requisito de usar el ÍNDICE y no un campo interno.
                            record_data['m_transaction_id'] = m_id
                            
                            # 4. Escribimos la fila individualmente.
                            #    Esto es más eficiente en memoria que crear una lista grande.
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
                        processed_line = self.process_log_line(line)
                        if processed_line:
                            yield processed_line
                    except Exception as e:
                        self.loggerfiles.error(
                            f'Error procesando línea {line_num} en {file_path}:\n'
                            f'Línea: {line[:200]}...\n'  # Primeros 200 caracteres
                            f'Error: {str(e)}'
                        )
        except Exception as e:
            self.loggerfiles.error(
                f'Error abriendo archivo {file_path}:\n'
                f'Error: {str(e)}'
            )
            raise

    def process_log_line(self, line):
        try:
            transaction_pattern = r"(transaction:)([^ ]*)"
            priority_pattern = r"pri:(\d+)"
            match = self.pattern.match(line)            

            if (match):
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
        except Exception as e:
            self.loggerfiles.error(
                f'Error en process_log_line:\n'
                f'Línea: {line[:200]}...\n'
                f'Error: {str(e)}'
            )
            return None

    def write_dataconfig(self):
        self.logger.info("VERSION 6.7.3.4")
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
        

    '''def monitor_resources(self):
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        
        self.logger.debug(
            f"Uso de memoria: {memory_info.rss / 1024 / 1024:.2f} MB\n"
            f"Buffers: {sum(len(b) for b in self.partition_buffers.values())} registros\n"
            f"FailOver records: {len(self.first_fo_records)} registros"
        )'''
      
if __name__ == "__main__":
    processed_data = []  # Lista para almacenar los resultados finales
    records_multisend = {}  # Diccionario para almacenar los registros de multisend

    try:
        manager = ProcessorFiles('./config/config.ini')        
        logging.info("Iniciando proceso.. ")
        
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
                if not data_line:  # Si no hay datos en la partición
                    manager.logger.debug(f"Partición vacía: {partition_file}")
                    continue
                    
                processed_data, records_multisend = manager.process_transactions(data_line)
                if processed_data:  # Si hay transacciones para escribir
                    manager.logger.debug(f"Partición {partition_file} procesada. Inicia proceso de escritura")
                    manager.write_in_threads(processed_data,records_multisend)
                    partitions_processed += 1
                
                # Liberación explícita de memoria
                del data_line
                del processed_data
                del records_multisend
                gc.collect()  # Forzar recolección de basura periódicamente
                
                # Reinicializar variables
                processed_data = []
                records_multisend = {}

            except Exception as e:
                manager.logger.error(f"Error procesando partición {partition_file}: {str(e)}")

        manager.logger.info(f"Resumen de particiones:")
        manager.logger.info(f"- Particiones procesadas: {partitions_processed}")
        manager.logger.info(f"- Particiones no encontradas: {partitions_not_found}")
        manager.logger.info(f"- Particiones totales esperadas: {manager.num_partitions}")
        
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