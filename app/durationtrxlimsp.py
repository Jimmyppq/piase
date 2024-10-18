import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import logging
import os
import configparser
import sys
import traceback
from collections import defaultdict

def load_config(config_file):
    """Carga la configuración desde un archivo .ini."""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DURATION']

def setup_logging(fecha_actual):
    
    output_logfile_path = config['LogFilePath']
    nombre_archivo, extension_archivo = os.path.splitext(output_logfile_path)   
    filenameLog = f"{nombre_archivo}_{fecha_actual}{extension_archivo}"

    # Configuración del logger principal
    logger = logging.getLogger('logger_principal')
    logger.setLevel(logging.INFO)
    # Handler para el primer archivo de log
    file_handler = logging.FileHandler(filenameLog)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Configuración del segundo logger
    output_logfile_path = config['LogReadFilePath']
    nombre_archivo, extension_archivo = os.path.splitext(output_logfile_path)   
    filenameLog = f"{nombre_archivo}_{fecha_actual}{extension_archivo}"

    otro_logger = logging.getLogger('otro_logger')
    otro_logger.setLevel(logging.INFO)
    # Handler para el segundo archivo de log
    otro_file_handler = logging.FileHandler(filenameLog)
    otro_file_handler.setFormatter(formatter)
    otro_logger.addHandler(otro_file_handler)
    
    # Configuración del tercer logger
    output_logfile_path = config['LogWriteFilePath']
    nombre_archivo, extension_archivo = os.path.splitext(output_logfile_path)   
    filenameLog = f"{nombre_archivo}_{fecha_actual}{extension_archivo}"

    logger_write = logging.getLogger('logger_write')
    logger_write.setLevel(logging.INFO)
    # Handler para el segundo archivo de log
    logger_writehandler = logging.FileHandler(filenameLog)
    logger_writehandler.setFormatter(formatter)
    logger_write.addHandler(logger_writehandler)

    # Configuración del logger transacciones descartadas
    output_logfile_path = config['LogDiscardedTrx']
    nombre_archivo, extension_archivo = os.path.splitext(output_logfile_path)   
    filenameLog = f"{nombre_archivo}_{fecha_actual}{extension_archivo}"

    logger_discarded = logging.getLogger('logger_discarded')
    logger_discarded.setLevel(logging.INFO)
    # Handler para el log de transacciones descartadas
    logger_discardedhandler = logging.FileHandler(filenameLog)
    logger_discardedhandler.setFormatter(formatter)
    logger_discarded.addHandler(logger_discardedhandler)

    # Escribir un mensaje en el log principal
    logger.info('--- Starting script ---')

    return logger, otro_logger, logger_write, logger_discarded

def compile_regular_expresion():
    global pattern
    pattern = re.compile(r"\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)")

def process_log_line(line):
    """
    Esta función procesa una línea de log y extrae los detalles relevantes.
    """
    global valid_actions,valid_subcomponents
    transaction_pattern = r"(transaction:)([^ ]*)"
    priority_pattern = r"pri:(\d+)"

    match = pattern.match(line)

    if match:
        details = match.groupdict()
        action = details['action']
        subcomponent = details['subcomponent']
        # Filtrar si el action no está en valid_actions y el subcomponent no está en valid_subcomponents
        if action not in valid_actions and subcomponent not in valid_subcomponents:
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

def log_file_generator(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            processed_line = process_log_line(line)
            if processed_line:
                yield processed_line

'''
##########process_transactions##############
    Procesa las transacciones de un archivo, aquí es dónde está toda la lógica 
    que unifica cada transacción en una sola línea, todo lo encontrado se va a escribiendo
    en un diccionario global que se va escribiendo a disco para "limpiar" la memoria cada X archivos
    determinado por la variable Chunk_size_write_files
'''
def process_transactions(file_path):
    global global_transactions
    for detail in log_file_generator(file_path):
        transaction_id = detail['transaction_id']
        timestamp = detail['timestamp']
        action = detail['action']
        subcomponent = detail['subcomponent']
        priority = detail.get('priority', -1)
        mtransaction_id = detail.get('Mtransaction_id')

        transaction = global_transactions.setdefault(transaction_id, {
            'date_min': timestamp,
            'date_max': timestamp,
            'first_action': action,
            'last_action': action,
            'first_subcomponent': subcomponent,
            'last_subcomponent': subcomponent,
            'priority': priority,
            'collector_times': [],
            'mnewtrans': mtransaction_id,
            'countSend': 0,
            'node_name': node_name,
            'send_times': []
        })

        update_min = timestamp < transaction['date_min'] and transaction['first_action'] != 'NEWTRANS'
        update_max = timestamp > transaction['date_max'] and transaction['last_action'] != 'SEND'

        if action == 'NEWTRANS' or update_min:
            transaction['date_min'] = timestamp
            transaction['first_action'] = action
            transaction['first_subcomponent'] = subcomponent
        elif action == 'SEND' or update_max:
            transaction['date_max'] = timestamp
            transaction['last_action'] = action
            transaction['last_subcomponent'] = subcomponent
        
        transaction['countSend'] += action == 'SEND'

        if action == 'SEND':
            transaction['send_times'].append(timestamp)

        if action == 'OUT' and subcomponent == 'FailOverManager':
            transaction['collector_times'].append(timestamp)

        if priority != -1:
            transaction['priority'] = priority

        if mtransaction_id is not None:
            transaction['mnewtrans'] = mtransaction_id

        

    cantidad_registros = len(global_transactions)
    if cantidad_registros > 0:
        logger_files.info(f"Processed {cantidad_registros} transactions from {file_path}")
    else:
        logger_files.warning('No transactions in this file')

def write_result(last_block):
    global archivoResultante
    global chunk_size
    totalRecords =0    
    block_chunk =1
    records = []
    cant_complete = 0
    completed_transactions = []

    logger_write.debug('Inicio de escritura')
    for trans_id, data in global_transactions.items():
        '''
        if data['send_times'] and data['collector_times']:
            record = {
                'Transaction ID': trans_id,
                'date_min': data['date_min'],
                'date_max': data['date_max'],
                'priority': data['priority'],
                'first_action': data['first_action'],
                'last_action': data['last_action'],
                'first_subcomponent': data['first_subcomponent'],
                'last_subcomponent': data['last_subcomponent'],
                'Duration': (data['date_max'] - data['date_min']).total_seconds(),
                'mnewtrans': data['mnewtrans'],
                'countSend': data['countSend'],
                'date_in_collector': data.get('date_in_collector'),
                'duration_limsp': None,
                'node_name': data['node_name'],
                'inot': False
            }
            records.append(record)
        '''
        
        send_times = data['send_times']
        collector_times = data['collector_times']
        
        if len(send_times) == len(collector_times) and len(send_times) > 0:
            for i, (send_time, collector_time) in enumerate(zip(send_times, collector_times)):
                inot_value = i > 0  # Marcar como True a partir del segundo SEND
                record = {
                    'Transaction ID': trans_id,
                    'date_min': data['date_min'],
                    'date_max': send_time,
                    'priority': data['priority'],
                    'first_action': data['first_action'],
                    'last_action': 'SEND',
                    'first_subcomponent': data['first_subcomponent'],
                    'last_subcomponent': data['last_subcomponent'],
                    'Duration': (send_time - data['date_min']).total_seconds(),
                    'mnewtrans': data['mnewtrans'],
                    'countSend': data['countSend'],
                    'date_in_collector': collector_time,
                    'duration_limsp': (collector_time - data['date_min']).total_seconds() if collector_time else None,
                    'node_name': data['node_name'],
                    'inot': inot_value
                }
                records.append(record)
                cant_complete +=1
            completed_transactions.append(trans_id)
        
        elif last_block:
            action = data['first_action']
            if action == 'NEWTRANS' :
                record = {
                'Transaction ID': trans_id,
                'date_min': data['date_min'],
                'date_max': data['date_max'],
                'priority': data['priority'],
                'first_action': data['first_action'],
                'last_action': data['last_action'],
                'first_subcomponent': data['first_subcomponent'],
                'last_subcomponent': data['last_subcomponent'],
                'Duration': (data['date_max'] - data['date_min']).total_seconds(),
                'mnewtrans': data['mnewtrans'],
                'countSend': data['countSend'],
                'date_in_collector': data.get('date_in_collector'),
                'duration_limsp': None,
                'node_name': data['node_name'],
                'inot': False
            }
                records.append(record)
                completed_transactions.append(trans_id)
                cant_complete +=1
        '''    
            Aquí se están descartando las transacciones cuando no tienen SEND, lo cual cuando se procesan
            transacciones por bloques no es del todo acertado ya que el SEND de la misma transacción puede 
            estar en un bloque mas adelante.
            Esta parte se podría enviar a una tabla en sql lite para mantener las transacciones incompletas, o 
            bien dejarlas en el diccionario original
            
            if discarded:
                logger_discarded.warning(f'Transaction discarded: {trans_id}')
        '''

        # Write the records to CSV in chunks
        if cant_complete >= chunk_size:
            logger_write.debug(f'Escribiendo {chunk_size} registros. block {block_chunk}')
            transactions_df = pd.DataFrame(records)
            transactions_df.to_csv(archivoResultante, mode='a', header=not os.path.exists(archivoResultante), index=False)
            logger_write.debug(f'Finalización escritura block {block_chunk}')
            records.clear()  # Clear the list to free memory
            block_chunk+=1            
            totalRecords +=1
    
    logger_principal.info('Vaciando memoria de transacciones completadas')
    for trans_id in completed_transactions:
        del global_transactions[trans_id]

    logger_principal.info(f'Se ha liberado memoria de {cant_complete} transacciones')

    # Write any remaining records
    if records:
        logger_write.debug(f'Escribiendo ultimo bloque {len(records)} ')
        transactions_df = pd.DataFrame(records)
        transactions_df.to_csv(archivoResultante, mode='a', header=not os.path.exists(archivoResultante), index=False)
        logger_write.debug('Finalización de proceso DF final chunk')

    logger_write.info(f"Total registros: {totalRecords} -- FileName: {archivoResultante}")
    logger_write.info(f"Transacciones que quedan en memoria {len (global_transactions)} ")

def orderbydate(directory_path,file_pattern):
    # Recorrer todos los archivos en el directorio, incluidos subdirectorios
    files = [file for file in Path(directory_path).rglob(file_pattern) if file.is_file()]
    
    # Ordenar los archivos por la fecha de modificación (de más antigua a más reciente)
    files_sorted = sorted(files, key=lambda file: file.stat().st_mtime)
    
    return files_sorted

def process_log_files(directory_path, file_pattern,chunk_files):
    global countFiles
    global file_name
    global node_name
    global archivoResultante
    
    
    directory_path = Path(directory_path)
    
    # Verificar si el archivo existe y eliminarlo si es necesario
    if os.path.exists(archivoResultante):
        os.remove(archivoResultante)
    
    matching_files = list(directory_path.rglob(file_pattern))
    
    if not matching_files:
        logging.error(f'No files found. Terminating the script. {directory_path} archivos {file_pattern}')
        sys.exit(1)
    
    logger_principal.info(f'File search results: {len(matching_files)} files...')
    filesbydate = orderbydate(directory_path,file_pattern)
    logger_principal.info("File read order:..")
    for i, file_path in enumerate(filesbydate):
        logger_principal.info(f"{i+1}: {file_path}")
    logger_principal.info("...File read order complete:")
    logger_principal.info('Starting processing log Files...')

    for file_path in filesbydate:
        file_path = Path(file_path)
        node_name = file_path.parent.name
        file_name = file_path.name
        path, file_name = os.path.split(file_path)
        logger_files.info(f'Nodo: {node_name} -- Archivo: {file_name} -- Path: {path}')   
        try:
            process_transactions(file_path)
            countFiles += 1
            if countFiles % chunk_files == 0:
                logger_principal.info(f'Ha terminado de procesar {countFiles} logs y se escribiran en el archivo final')
                write_result(False)
                logger_principal.info('Escritura finalizada')
                #global_transactions.clear() #vaciar memoria de transacciones acumuladas

            if countFiles % 20 == 0:
                logger_principal.info(f'Ha terminado de procesar {countFiles} archivos…')
        except Exception as e:
            logger_files.error(f'Error processing file {file_name}: {e}')
            #sys.exit(1)
        
    # Escribir el archivo CSV con todas las transacciones restantes
    write_result(True)
    trx_incomplete = len(global_transactions)
    if trx_incomplete > 0 :
        logger_principal.info(f'Se han descartado {trx_incomplete} transacciones')
        if discarded:
            for trans_discard_id in global_transactions.items():
                logger_discarded.warning(f'Transaction discarded: {trans_discard_id}')
            


global_transactions = defaultdict(lambda: {
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
})

archivoResultante =""

def validate_write_access(output_result, logger):
    """Valida si se puede escribir un archivo en la ruta especificada."""
    try:
        # Intenta crear y escribir en el archivo especificado
        with open(output_result, 'w') as test_file:            
            test_file.write("Validación de acceso de escritura.")
            logger.info(f"Validación de escritura exitosa en: {output_result}")
        
        # Elimina el contenido escrito para la validación
        os.remove(output_result)
    except Exception as e:
        # Registra el error y sale de la aplicación
        logger.error(f"Error intentando escribir el archivo en {output_result}: {e}")
        logger.error(traceback.format_exc())
        exit(1)


def write_dataconfig (logger,chunk_size,chunk_files,discarded,filePattern,inputFile ):
    logger.info ("VERSION 6.4 (duration)")
    logger.info (f"Chunk_size_write_files: {chunk_files}")
    logger.info (f"Chunk_size_write: {chunk_size}")
    logger.info (f"writeDiscarded: {discarded}")
    logger.info (f"FilePattern: {filePattern}")
    logger.info (f"InputFile: {inputFile}")   


#main
try:
    config = load_config('./config/config.ini')
except FileNotFoundError as e:
    print(f"Error cargando la configuración: Archivo no encontrado: {e}")
    sys.exit(1)
except KeyError as e:
    print(f"Error cargando la configuración: Clave faltante en el archivo de configuración: {e}")
    sys.exit(1)
except Exception as e:
    print(f"Error inesperado cargando la configuración: {e}")
    sys.exit(1)

fecha_actual = datetime.now().strftime("%d%m%Y")
logger_principal, logger_files, logger_write, logger_discarded = setup_logging(fecha_actual)
compile_regular_expresion()
archivoResultante = config['OutputFilePath']
chunk_size = int(config['Chunk_size_write'])
chunk_files = int(config['Chunk_size_write_files'])
filePattern = config['FilePattern']
inputFile = config['InputPath']
countFiles = 0
   
try:
    discarded = config.get('writeDiscarded', 'False')  # Por defecto es una cadena 'False'
    discarded = discarded.lower() in ('true', '1', 'yes', 'on')  # Convierte a booleano
except KeyError as e:
    discarded = False

write_dataconfig(logger_principal,chunk_size,chunk_files,discarded,filePattern,inputFile)
validate_write_access (archivoResultante,logger_principal)
# Convertir los valores a sets para hacer las búsquedas más rápidas
valid_actions = set(config['valid_actions'].split(','))
valid_subcomponents = set(config['valid_subcomponents'].split(','))

try:
    process_log_files(inputFile, filePattern, chunk_files )
except Exception as e:
    logger_principal.error(f'Error processing log files: {e}')
    logger_principal.error(traceback.format_exc())
    sys.exit(1)

logger_principal.info('Processing completed…')
logger_principal.info('Saving results…')
logger_principal.info(f'Result stored {archivoResultante}')
sys.exit(0)