import csv
import os
import pickle
import logging
import configparser
import time
import sys
import threading
from datetime import datetime

class BinaryTransactionReader:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        if 'LOGGING' not in self.config:
            raise KeyError("'LOGGING' section not found in the configuration file.")
        
        self.setup_logging()        
      
        self.transactions_incomplete = []
        self.transactions_complete = []
        self.group_trx_complete_towrite = []
        self.index_incomplete = {}
        self.count_trx_write = 0
        self.count_actualizadas = 0  
        self.count_process_complete = 0

        #Define el numero del lote de indice
        self.index_block = 0

        #cuando el archivo de incompletas no existe (posiblemente solo suceda en entornos de prueba) 
        #se pone False para que no se cree el indice y posteriormente no se utilice para escribir el binario   
        self.exist_incomplete_transactiones = True 
        self.keep_running = True
        
        self.load_configuration_values()
        self.write_dataconfig()
        
    def load_configuration_values(self):
        if 'PROCESS_FILES' not in self.config:
            self.logger.error("'PROCESS_FILES' section not found in the configuration file.")
            raise KeyError("'PROCESS_FILES' section not found in the configuration file.")
        # Archivo con las transacciones incompletas, de preferencia han de ser las que genera el "reviewIncomplete.py"
        # funcionaría con el archivo de incompletas de duration, pero habrian varias incompletas no reales
        self.incomplete_transactions_file = self.config['PROCESS_FILES']['IncompleteReviewTransactionsFile']
        self.completed_transactions_file = self.config['PROCESS_FILES']['CompletedTransactionsFile']
        self.reviewTransactionsFile = self.config['PROCESS_FILES']['ReviewTransactionsFile']
        self.resultFinalFile = self.config['PROCESS_FILES']['ResultFinalFile']
        self.timeToLog = self.config['PROCESS_FILES'].getint('timeToLog', fallback=60)

     
    def load_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    def setup_logging(self):
        log_level_str = self.config['LOGGING'].get('LogLevel', 'INFO').upper()
        log_level = getattr(logging, log_level_str, logging.INFO)

        # Logger principal con fecha en el nombre del archivo
        log_file_path = self.config['LOGGING']['LogFilePathReadBinary']
        log_file_path_with_date = f"{os.path.splitext(log_file_path)[0]}_{datetime.now().strftime('%Y-%m-%d')}{os.path.splitext(log_file_path)[1]}"
        logging.basicConfig(filename=log_file_path_with_date,
                            level=log_level,
                            format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger()

    def process_batch_incomplete_transactions(self):
        progress_thread = threading.Thread(target=self.log_progress, daemon=True)
        progress_thread.start()
        block_process = 1


        # Cantidad de transacciones que se van acumulando en la lista transactions_complete antes de ser enviadas al método
        # process_transactions. Cada que se envian a este método este se recorre todos los indices, por eso se debe minimizar
        # todo lo posible la invocación de este método 
        count_trx_complete_load = 0
        try:
            with open(self.completed_transactions_file, 'rb') as file:
                while True:
                    try:
                        transactions = pickle.load(file)
                        count_trx_complete_load += len(transactions)
                        self.transactions_complete.extend(transactions)
                        if count_trx_complete_load > 3000000:                                                
                            self.process_transactions(block_process)
                            self.transactions_complete.clear()
                            self.logger.debug(f'Bloque de procesamiento completado {block_process}')  
                            block_process +=1
                            count_trx_complete_load = 0
                    except EOFError:
                        break
        except Exception as e:
            self.logger.error(f"Error al leer las transacciones incompletas del archivo binario: {e}")
            sys.exit(1)

        if self.transactions_complete :
            self.process_transactions(block_process)
            self.logger.debug(f'Bloque de procesamiento final {block_process}') 

        if self.group_trx_complete_towrite :
            self.logger.debug('last block...')                        
            self.write_binary_send_review()
            

        self.logger.debug('terminando procesamiento de transacciones incompletas')
        self.keep_running = False
        progress_thread.join()
   
    def process_transactions(self, block_process):
        

        self.logger.debug(f'Se van a procesar {len(self.transactions_complete)} trxs - block: {block_process}')        
        if self.exist_incomplete_transactiones :
            for i in range(self.index_block):
                self.load_index(i+1) 
                for complete_transaction in self.transactions_complete:  
                    self.count_process_complete += 1                         
                    transaction_id_complete = complete_transaction['Transaction ID']
                    if transaction_id_complete  in self.index_incomplete:
                        transaction_incomplete_details = self.index_incomplete[transaction_id_complete]
                        complete_transaction['Date Min'] = transaction_incomplete_details['Date Min']
                        complete_transaction['first_action'] = transaction_incomplete_details['first_action']
                        complete_transaction['first_subcomponent'] = transaction_incomplete_details['first_subcomponent']
                        self.count_actualizadas +=1
                        self.group_trx_complete_towrite.append(complete_transaction)                
                        continue
                    if self.count_process_complete % 700000 == 0 :                
                        #ejecutado así para pruebas, pero procesar en un hilo, se deja documentado por sino se alcanza a probar que luego no se olvide
                        self.write_binary_send_review()             
                        self.logger.debug('Escritura finalizada')                
                    self.group_trx_complete_towrite.append(complete_transaction)
                    
            if self.group_trx_complete_towrite :
                self.write_binary_send_review() 

            self.logger.debug(f'Procesamiento terminado ({block_process})') 
            return

        for complete_transaction in self.transactions_complete: 
            self.count_process_complete += 1
            self.group_trx_complete_towrite.append(complete_transaction)
            self.write_binary_send_review()             
        
                
    def log_progress(self):
        while self.keep_running:
            self.logger.info(f"(th)Transacciones procesadas hasta ahora: {self.count_process_complete} y encontradas como incompletas {self.count_actualizadas}")                   
            time.sleep(self.timeToLog)  # Esperar x segundos
        

    def create_index(self):
        """
        Crea un índice de transacciones incompletas si el archivo existe.
        """
        if not os.path.exists(self.incomplete_transactions_file):
            self.logger.warning(f"El archivo de transacciones incompletas {self.incomplete_transactions_file} no existe. No se creará un índice.")
            self.exist_incomplete_transactiones = False
            return  # Salir del método si el archivo no existe

        index = {}
        # Cantidad de transacciones indexadas, solo se indexan transacciones con SEND. Se utiliza al final para informar en un log cuantas trx ha indexado
        count_trx = 0 

        #Cantidad de transacciones indexadas en un grupo de carga, es necesario reiniciarlo cada que llegue al umbral definido por eso no se puede utilizar el anterior
        count_trx_limit = 0 

        

        
        self.logger.info('Creando índice trx incomplete')
        try:
            with open(self.incomplete_transactions_file, 'rb') as file:
                while True:
                    try:
                        transactions = pickle.load(file)
                        for transaction in transactions:                            
                            transaction_id = transaction['Transaction ID']
                            # Se indexaran unicamente las transacciones que tengan envio registrado
                            # Esto dado que solamente las que aparezcan como incompletas y enviadas, seran
                            # buscadas dentro del archivo de transacciones completadas para adicionarlas
                            if transaction['first_action'] == 'SEND' or transaction['Last Action'] == 'SEND':
                                count_trx += 1
                                count_trx_limit += 1
                                index[transaction_id] = {
                                    'Date Min': transaction['Date Min'],
                                    'first_action': transaction['first_action'],
                                    'first_subcomponent': transaction['first_subcomponent']
                                }
                                                                 
                        if count_trx_limit > 7000000 :                            
                            try:
                                self.index_block +=1
                                index_filename = f'./output/transactions_index{self.index_block}.pkl'
                                #Si el arhivo existe se sobre-escribe (w)
                                with open(index_filename, 'wb') as index_file:
                                    pickle.dump(index, index_file)
                                self.logger.info(f"Índice {self.index_block} guardado  con éxito. Transacciones indexadas: {count_trx_limit}")
                                count_trx_limit = 0
                                index.clear()
                            except Exception as e:
                                self.logger.error(f"Error al guardar el índice {self.index_block}: {e}")
                                sys.exit(1)  
                    except EOFError:
                        break
        except Exception as e:
            self.logger.error(f"Error al crear el índice {self.index_block} del archivo binario: {e}")
            sys.exit(1)
        self.logger.info(f'Total de transacciones indexadas {count_trx}')



    def load_index(self,block_number):
        
        self.index_incomplete.clear()        
        file_name_index = f'./output/transactions_index{block_number}.pkl'
        self.logger.info(f'Cargando indice {block_number} en memoria: {file_name_index}')
        try:
            with open(file_name_index, 'rb') as index_file:
                self.index_incomplete = pickle.load(index_file)            
            self.logger.info(f"Índice {block_number} cargado en memoria con éxito. {len(self.index_incomplete)}")
        except Exception as e:
            self.logger.error(f"Error al cargar el índice del archivo binario: {e}")
            sys.exit(1)  # Detener la ejecución si el índice no se puede cargar
        
   # Escribe las el contenido de la lista group_trx_complete_towrite el cual cntiene las transacciones  
   # completadas que se han modificado previamente para complementarla con el indice de incompletas si es que existian. 
    def write_binary_send_review(self) :
        self.logger.debug('Inicia escritura de bloque de transacciones revisadas')
        trx_to_write = 0
        try:
            with open(self.reviewTransactionsFile, 'ab') as bin_file:  # 'ab' para agregar datos en formato binario
                pickle.dump(self.group_trx_complete_towrite, bin_file)
            trx_to_write=len(self.group_trx_complete_towrite)
            self.logger.debug(f"{trx_to_write} nuevas transacciones cerradas escritas en el archivo review")
            self.count_trx_write += trx_to_write          
        except Exception as e:
            self.logger.error(f"Error al escribir las transacciones incompletas al archivo binario: {e}")
        finally :
            self.group_trx_complete_towrite.clear()

    def write_binary_to_csv(self):
        """
        Convierte un archivo binario con transacciones en un archivo CSV.
        Procesa en bloques para mejorar el rendimiento y manejar grandes archivos.
        """
        file_to_process = ""
        # Verificar existencia del archivo binario
        if not os.path.exists(self.reviewTransactionsFile):
            self.logger.error(f"No se encuentra el archivo binario para escribir el CSV: {self.reviewTransactionsFile}. Se utilizara el archivo de completadas inicial sin cambios")
            file_to_process = self.completed_transactions_file
        else :
            file_to_process = self.reviewTransactionsFile

        try:
            with open(file_to_process, 'rb') as binary_file, open(self.resultFinalFile, 'a', newline='') as csv_file:
                csv_writer = csv.DictWriter(csv_file, fieldnames=[
                    'Transaction ID', 'Date Min', 'date_max', 'Priority',
                    'first_action', 'first_subcomponent', 'Last Action', 'Last Subcomponent',
                    'countSend', 'date_in_collector', 'Duration', 'duration_limsp', 'NodeName', 'Filename'
                ])
                
                # Escribir el encabezado si el archivo CSV está vacío
                if os.stat(self.resultFinalFile).st_size == 0:
                    csv_writer.writeheader()
                
                # Procesar el archivo binario
                while True:
                    try:
                        # Leer el siguiente bloque del archivo binario
                        transactions = pickle.load(binary_file)
                        
                        # Verificar que sea una lista de diccionarios
                        if not isinstance(transactions, list):
                            self.logger.error(f"Formato inesperado: {type(transactions)} en el archivo binario.")
                            continue
                        
                        # Escribir los datos en el archivo CSV
                        csv_writer.writerows([t for t in transactions if isinstance(t, dict)])
                    except EOFError:
                        break  # Fin del archivo binario
                    except Exception as e:
                        self.logger.error(f"Error al procesar transacciones del archivo binario: {e}")
        except Exception as e:
            self.logger.error(f"Error general al convertir el binario a CSV: {e}")

    def clear_binary_file(self):
        """
        Elimina el archivo binario si existe, para evitar acumular datos de ejecuciones anteriores.
        """
        if os.path.exists(self.reviewTransactionsFile):
            try:
                os.remove(self.reviewTransactionsFile)
                self.logger.info(f"Archivo binario {self.reviewTransactionsFile} eliminado correctamente.")
            except Exception as e:
                self.logger.error(f"Error al intentar eliminar el archivo binario {self.reviewTransactionsFile}: {e}")
                sys.exit(1)  # Detener la ejecución si no se puede limpiar el archivo binario
        else:
            self.logger.info(f"El archivo binario {self.reviewTransactionsFile} no existía, no es necesario eliminarlo.")

    def write_dataconfig(self):
        self.logger.info("VERSION 1.7.3")
        self.logger.info(f"IncompleteReviewTransactionsFile: {self.incomplete_transactions_file}")
        self.logger.info(f"CompleteTransactionsFile: {self.completed_transactions_file}")
        self.logger.info(f"ReviewTransactionsFile: {self.reviewTransactionsFile}")        
        self.logger.info(f"timeToLog: {self.timeToLog}")
        self.logger.info(f"resultFinalFile: {self.resultFinalFile}")

if __name__ == "__main__":
    start_time = time.time()
    
    count_trx_write = 0
    try:
        reader = BinaryTransactionReader('./config/config.ini')
        reader.clear_binary_file()
        reader.create_index()
        reader.process_batch_incomplete_transactions()
        reader.write_binary_to_csv()

        count_trx_write = reader.count_trx_write
        #reader.write_transactions_to_csv(reader.transactions_incomplete,'./output/allIncomplete.csv')
        #reader.read_completed_transactions()
        #reader.process_batch_incomplete_transactions()
        
        #reader.process_transactions()
        #reader.count_lines_in_file(reader.incomplete_transactions_file)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        end_time = time.time()
        total_time = end_time - start_time
        logging.info(f'Transacciones escritas en review {count_trx_write}')
        if total_time > 60 :
            logging.info(f"Tiempo total: {total_time / 60:.2f} minutos.")
        else:
            logging.info(f"Tiempo total: {total_time:.2f} segundos.")
