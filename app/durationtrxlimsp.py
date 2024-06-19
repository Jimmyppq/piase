import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import logging
import os
import configparser
from collections import defaultdict

def load_config(config_file):
    """Carga la configuración desde un archivo .ini."""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DURATION']

def setup_logging():
    # Configuración del logger principal
    logger = logging.getLogger('logger_principal')
    logger.setLevel(logging.INFO)
    # Handler para el primer archivo de log
    file_handler = logging.FileHandler(config['LogFilePath'])
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Configuración del segundo logger
    otro_logger = logging.getLogger('otro_logger')
    otro_logger.setLevel(logging.INFO)
    # Handler para el segundo archivo de log
    otro_file_handler = logging.FileHandler(config['LogReadFilePath'])
    otro_file_handler.setFormatter(formatter)
    otro_logger.addHandler(otro_file_handler)
    
    # Configuración del tercer logger
    logger_write = logging.getLogger('logger_write')
    logger_write.setLevel(logging.INFO)
    # Handler para el segundo archivo de log
    logger_writehandler = logging.FileHandler(config['LogWriteFilePath'])
    logger_writehandler.setFormatter(formatter)
    logger_write.addHandler(logger_writehandler)

    # Escribir un mensaje en el log principal
    logger.info('--- Starting script ---')

    return logger, otro_logger, logger_write

def compile_regular_expresion ():
    global pattern
    #compilar la expresión regular
    pattern = re.compile(r"\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)")

def process_log_line(line):
    """
    This function processes a log line and extracts relevant details.
    """
    # Regular expression for finding transaction ID and priority
    transaction_pattern = r"(transaction:)([^ ]*)"
    priority_pattern = r"pri:(\d+)"

    # Parse the log line
    match = pattern.match(line)

    if match:
        # Extract details
        details = match.groupdict()

        # Find transaction ID if present
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

        # Find priority if present
        priority_match = re.search(priority_pattern, details['details'])
        if priority_match:
            details['priority'] = int(priority_match.group(1))
        else:
            details['priority'] = -1

        if '.' in details['timestamp']:
            # Format with milliseconds
            details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S.%f")
        else:
            # Format without milliseconds
            details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S")

        return details

    return None

def log_file_generator(file_path):
    # Generador para leer y procesar archivos de log
    with open(file_path, 'r') as file:
        for line in file:
            processed_line = process_log_line(line)
            if processed_line:  # Filtra líneas que no contienen un ID de transacción
                yield processed_line

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
            'send_times': []
        })

        update_min = timestamp < transaction['date_min'] and transaction['first_action'] != 'NEWTRANS'
        update_max = timestamp > transaction['date_max'] and transaction['last_action'] != 'SEND'

        if action == 'NEWTRANS' or update_min:
            transaction['date_min'] = timestamp
            transaction['first_action'] = action
            transaction['first_subcomponent'] = subcomponent

        if action == 'SEND' or update_max:
            transaction['date_max'] = timestamp
            transaction['last_action'] = action
            transaction['last_subcomponent'] = subcomponent
        
        transaction['countSend'] += action == 'SEND'

        if action == 'SEND':
            transaction['send_times'].append(timestamp)

        if priority != -1:
            transaction['priority'] = priority

        if mtransaction_id is not None:
            transaction['mnewtrans'] = mtransaction_id

        if action == 'OUT' and subcomponent == 'FailOverManager':
            transaction['collector_times'].append(timestamp)

    cantidad_registros = len(global_transactions)
    if cantidad_registros > 0:
        logger_files.info(f"Processed {cantidad_registros} transactions from {file_path}")
    else:
        logger_files.warning('No transactions in this file')

def write_result():
    records = []
    for trans_id, data in global_transactions.items():
        if not data['send_times'] and not data['collector_times']:
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
                'node_name': node_name,
                'inot': False
            }
            records.append(record)
        
        send_times = data['send_times']
        collector_times = data['collector_times']
        
        if len(send_times) == len(collector_times):
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
                    'node_name': node_name,
                    'inot': inot_value
                }
                records.append(record)
        else:
            logger_files.warning(f'Mismatch in the number of send_times and collector_times for transaction ID: {trans_id}')

    transactions_df = pd.DataFrame(records)

    transactions_df.to_csv(config['OutputFilePath'], mode='w', header=True, index=False)
    logger_write.info(f"Total Records: {transactions_df.shape[0]} -- FileName: {config['OutputFilePath']}")


def process_log_files(directory_path, file_pattern):
    global countFiles
    global file_name
    global node_name
    
    logger_principal.info('Starting processing log Files...')
    directory_path = Path(directory_path)
    
    matching_files = list(directory_path.rglob(file_pattern))
    
    if not matching_files:
        logging.error('No files found. Terminating the script.')
    
    logger_principal.info(f'File search results: {len(matching_files)} files...')
    for file_path in directory_path.rglob(file_pattern):
        file_path = Path(file_path)
        node_name = file_path.parent.name
        file_name = file_path.name
        path, file_name = os.path.split(file_path)
        logger_files.info(f'Nodo: {node_name} -- Archivo: {file_name} -- Path: {path}')        
        process_transactions(file_path)       
        countFiles += 1
        if countFiles % 20 == 0:
            logger_principal.info(f'Ha terminado de procesar {countFiles} archivos...')
    
    # Verificar si el archivo existe y eliminarlo si es necesario
    if os.path.exists(config['OutputFilePath']):
        os.remove(config['OutputFilePath'])
    
    # Escribir el archivo CSV con todas las transacciones acumuladas
    write_result()


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


#main
config = load_config('./config/config.ini')
logger_principal, logger_files, logger_write = setup_logging()
compile_regular_expresion()

countFiles = 0

process_log_files(config['InputPath'], config['FilePattern'] )

logger_principal.info('Processing completed…')
logger_principal.info('Saving results…')
archivoResultante = config['OutputFilePath']
logger_principal.info(f'Result stored {archivoResultante}')