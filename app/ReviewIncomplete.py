import os
import pickle
import logging
import configparser
import time
import sys
import threading
from datetime import datetime
from collections import defaultdict

class ReviewIncomplete:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        if 'LOGGING' not in self.config:
            raise KeyError("'LOGGING' section not found in the configuration file.")
        
        self.results = [] # Lista para almacenar los resultados finales
        self.result_incomplete = [] # Lista para almacenar las nuevas transacciones incomppletas
           
        self.start_time = time.time()        
        self.count_trx_complete = 0
        self.count_trx_incomplete = 0
        self.count_trx_read = 0
        self.count_trx_process = 0
        self.keep_running = True # establece cuando detener el hilo que escribe los logs
        self.transaction_dict = defaultdict(list)
        
        self.setup_logging()
        self.load_configuration_values()
        self.write_dataconfig()

    def load_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config
    
    def setup_logging(self):
        log_level_str = self.config['LOGGING'].get('LogLevel', 'INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)

        # Logger principal con fecha en el nombre del archivo
        log_file_path = self.config['LOGGING'].get('LogFilePathIncomplete','./logs/log_processIncomplete')
        log_file_path_write = self.config['LOGGING'].get('LogFilePathIncomplete_write', './logs/log_processIncomplete_write.log')

        log_file_path_with_date = f"{os.path.splitext(log_file_path)[0]}_{datetime.now().strftime('%Y-%m-%d')}{os.path.splitext(log_file_path)[1]}"
        log_file_path_write_with_date = f"{os.path.splitext(log_file_path_write)[0]}_{datetime.now().strftime('%Y-%m-%d')}{os.path.splitext(log_file_path_write)[1]}"

        logging.basicConfig(filename=log_file_path_with_date,
                            level=log_level,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()

        file_handler_write = logging.FileHandler(log_file_path_write_with_date)
        file_handler_write.setLevel(log_level)
        file_handler_write.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

        self.logger_write = logging.getLogger('logger_write')
        self.logger_write.addHandler(file_handler_write)
        self.logger_write.setLevel(log_level)
        self.logger_write.propagate = False     

    def load_configuration_values(self):
        if 'PROCESS_FILES' not in self.config:
            self.logger.error("'PROCESS_FILES' section not found in the configuration file.")
            raise KeyError("'PROCESS_FILES' section not found in the configuration file.")
        
        self.incomplete_transactions_file = self.config['PROCESS_FILES']['IncompleteTransactionsFile']
        self.CompletedTransactionsFile = self.config['PROCESS_FILES']['CompletedTransactionsFile']
        self.incomplete_transactions_review_file = self.config['PROCESS_FILES']['IncompleteReviewTransactionsFile'] #Las nuevas incompletas que generará esta clase
        self.timeToLog = self.config['PROCESS_FILES'].getint('timeToLog', fallback=60)
        self.chunk_size = self.config['PROCESS_FILES'].getint('Chunk_size', fallback=5000000)        
        self.mem_trx_security = self.chunk_size * 0.1        
               
    def process_incomplete(self) :
        block = 0
        try:            
            progress_thread = threading.Thread(target=self.log_progress, daemon=True)
            progress_thread.start()
            if not os.path.exists(self.incomplete_transactions_file):
                self.logger.error(f"El archivo {self.incomplete_transactions_file} no existe. Terminando la ejecución.")
                sys.exit(1)  # Terminar el programa con un código de error
                            
            with open(self.incomplete_transactions_file, 'rb') as file:
                while True:
                    try:
                        transactions = pickle.load(file)
                        self.logger.debug(f'transacciones incompletas cargadas {len(transactions)} - bloque {block}')
                        
                        for transaction in transactions:
                            transaction_id = transaction['Transaction ID']
                            self.transaction_dict[transaction_id].append({
                                'Date Min': transaction['Date Min'],
                                'date_max': transaction['date_max'],
                                'Priority': transaction['Priority'],
                                'first_action': transaction['first_action'],
                                'first_subcomponent': transaction['first_subcomponent'],
                                'Last Action': transaction['Last Action'],
                                'Last Subcomponent': transaction['Last Subcomponent'],
                                'countSend': transaction['countSend'],
                                'date_in_collector': transaction['date_in_collector'],
                                'Duration': transaction['Duration'],
                                'duration_limsp': transaction['duration_limsp'],
                                'NodeName': transaction['NodeName'],
                                'Filename': transaction['Filename'],
                                'm_transaction_id': transaction['m_transaction_id']
                            })
                        block +=1
                        self.logger.debug(f'Bloque de lectura: {block}')
                        self.process_transaction_block()
                        

                    except EOFError:
                        break

            self.logger.debug('Fin de la lectura del archivo de transacciones incompletas')                
            if self.results :
                self.logger.debug('Transacciones completas en el último bloque') 
                thread_complete = threading.Thread(target=self.write_result_to_binary, daemon=True)
                thread_complete.start()   
            if self.result_incomplete :
                self.logger.debug('Transacciones incompletas en el último bloque') 
                thread_incomplete = threading.Thread(target=self.write_result_incomplete_to_binary, daemon=True)
                thread_incomplete.start()

            self.keep_running = False
            progress_thread.join()
            if hasattr(self, 'thread_complete') and self.thread_complete.is_alive():
                self.thread_complete.join()
            if hasattr(self, 'thread_incomplete') and self.thread_incomplete.is_alive():
                self.thread_incomplete.join()
        except Exception as e:
            self.logger.error(f"Error al leer las transacciones incompletas del archivo binario: {e}")
            sys.exit(1)

    def process_transaction_block(self):
        self.count_trx_read +=  len(self.transaction_dict)
        self.logger.debug(f'Se reciben {len(self.transaction_dict)} trx para procesar')
        trx_out = trx_in = False
        

        if hasattr(self, 'thread_complete') and self.thread_complete.is_alive():
            self.thread_complete.join()
        if hasattr(self, 'thread_incomplete') and self.thread_incomplete.is_alive():
            self.thread_incomplete.join()

        for transaction_id, records in self.transaction_dict.items():            
            result = {
                'Transaction ID': transaction_id,
                'Date Min': records[0]['Date Min'],
                'date_max': records[0]['date_max'],
                'Priority': records[0]['Priority'],
                'first_action': records[0]['first_action'],
                'first_subcomponent': records[0]['first_subcomponent'], 
                'Last Action': records[0]['Last Action'],
                'Last Subcomponent': records[0]['Last Subcomponent'],
                'countSend': records[0]['countSend'],
                'date_in_collector': records[0]['date_in_collector'],
                'Duration': records[0]['Duration'],
                'duration_limsp': records[0]['duration_limsp'],
                'NodeName': records[0]['NodeName'],                
                'Filename': records[0]['Filename'], 
                'm_transaction_id': records[0]['m_transaction_id']
                }
            for record in records:        
                first_action = record['first_action']
                date_min = record['Date Min']
                first_subcomponent = record['first_subcomponent']
                last_action = record ['Last Action']
                date_max = record ['date_max']
                last_subcomponent = record['Last Subcomponent']
                date_in_collector = record['date_in_collector']

                if first_action == 'NEWTRANS':
                    result['first_action']=first_action
                    result['Date Min']=date_min
                    result['first_subcomponent']=first_subcomponent
                    trx_in = True
                elif first_action == 'SEND':
                    result['Last Action']='SEND'
                    result['date_max'] = date_min
                    result['Last Subcomponent'] = first_subcomponent
                    trx_out = True
                    
                if last_action == 'SEND':
                    result['Last Action']='SEND'
                    result['date_max'] = date_max
                    result['Last Subcomponent'] = last_subcomponent
                    trx_out = True

                if first_subcomponent == 'FailOverManager':
                    result['date_in_collector']=date_in_collector
                    
          
            if trx_in and trx_out : 
                result['Duration'] = (result['date_max'] - result['Date Min']).total_seconds()
                result['duration_limsp'] = (result['date_in_collector'] - result['Date Min']).total_seconds() if result['date_in_collector'] else None
                self.results.append(result)
            else:                
                self.result_incomplete.append(result)

            trx_out = trx_in = False
            self.count_trx_process +=1

        if self.count_trx_process > self.mem_trx_security :
            self.write_in_threads()
            self.count_trx_process = 0
        
        self.transaction_dict.clear()

    

    def write_in_threads(self):
        # Crear los hilos
        self.thread_complete = threading.Thread(target=self.write_result_to_binary, daemon=True)
        self.thread_incomplete = threading.Thread(target=self.write_result_incomplete_to_binary, daemon=True)
        
        # Iniciar los hilos
        self.thread_complete.start()
        self.thread_incomplete.start()

        # Esperar a que los hilos terminen si es necesario
        #thread_complete.join()
        #thread_incomplete.join()

    def write_result_to_binary(self):
        try:
            if not self.results:  # Verifica si self.results está vacío
                self.logger_write.debug('No hay transacciones completas para escribir.')
                return
            
            self.logger_write.debug('Inicio de escritura Completas')
            with open(self.CompletedTransactionsFile, 'ab') as bin_file:  # 'ab' para agregar datos en formato binario
                pickle.dump(self.results, bin_file)
            self.logger_write.debug(f"{len(self.results)} transacciones completas {self.CompletedTransactionsFile}")
            self.count_trx_complete += len(self.results)
            self.results.clear()
        except Exception as e:
            self.logger_write.error(f"Error al escribir transacciones completadas al archivo: {e}")

    def write_result_incomplete_to_binary(self):
        try:
            if not self.result_incomplete:  # Verifica si self.result_incomplete está vacío
                self.logger_write.debug('No hay transacciones incompletas para escribir.')
                return
            
            self.logger_write.debug('Inicio de escritura Incompletas')
            with open(self.incomplete_transactions_review_file, 'ab') as bin_file:  # 'ab' para agregar datos en formato binario
                pickle.dump(self.result_incomplete, bin_file)
            self.logger_write.debug(f"{len(self.result_incomplete)} Transacciones incompletas {self.incomplete_transactions_review_file}")
            self.count_trx_incomplete += len(self.result_incomplete)
            self.result_incomplete.clear()
        except Exception as e:
            self.logger_write.error(f"Error al escribir transacciones incompletas al archivo: {e}")


    def log_progress(self):
        while self.keep_running:
            parcial_time = time.time()
            total_time = parcial_time - self.start_time   
            self.logger.info(f'(tmp)Trx completas {process.count_trx_complete}. Trx icompletas {process.count_trx_incomplete}. Trx leídas {process.count_trx_read}')       
            #self.logger.info(f"Total de transacciones marcadas como completadas {self.count_trx_complete}, y transacciones leídas {self.count_trx_read}")                               
            if total_time > 3600 :
                logging.info(f"(tmp)Tiempo transcurrido: {total_time / 3600:.2f} horas.")
            else:
                logging.info(f"(tmp)Tiempo transcurrido: {total_time / 60:.2f} minutos.")
            time.sleep(self.timeToLog)  # Esperar x segundos

    def write_dataconfig(self):
        self.logger.info("VERSION 2.0-b")
        self.logger.info(f"IncompleteTransactionsFile: {self.incomplete_transactions_file}")
        self.logger.info(f"CompleteTransactionsFile: {self.CompletedTransactionsFile}")
        self.logger.info(f"IncompleteReviewTransactionsFile: {self.incomplete_transactions_review_file}")        
        self.logger.info(f"timeToLog: {self.timeToLog}")
        self.logger.info(f"mem_trx_security: {self.mem_trx_security}")

    
if __name__ == "__main__":
        
    try:
        process = ReviewIncomplete('./config/config.ini')
        
        process.process_incomplete()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        end_time = time.time()
        total_time = end_time - process.start_time
        logging.info(f'Transacciones completas {process.count_trx_complete}')
        logging.info(f'Transacciones incompletas {process.count_trx_incomplete}')
        logging.info(f'Transacciones leídas {process.count_trx_read}')       

        if total_time > 60 :
            logging.info(f"Tiempo total: {total_time / 60:.2f} minutos.")
        else:
            logging.info(f"Tiempo total: {total_time:.2f} segundos.")

