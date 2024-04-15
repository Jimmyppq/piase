import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import logging
import json
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
    
    # Configuración del segundo logger
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
    # Regular expression for parsing the log line
    #pattern = r"\[(?P<timestamp>.+?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    #pattern = r"(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    ########################################################################################################################################################
    ### here ####pattern = r"\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    #[2021/12/15 23:12:16] TOREGISTER ServiceCenterOu transaction:ULr/+jE3vcAZNPFumuwl7s+X
    #[2021/12/15 23:12:16] OUT        ServiceCenterOu transaction:ULr/+jE3vcAZNPFumuwl7s+X pri:4
    ########################################################################################################################################################

    #pattern = r"(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    #2023/08/15 21:57:56 OUT        EventManager    event:UMbw23DMS0kIqe41BxllezcS
    #2023/08/15 21:57:56 IN         WsSerInAdpAuth  transaction:UMbw23SQkLxRJu41Bxktkxuo
    #2023/08/15 21:57:56 NEWTRANS   WsSerInAdpAuth  transaction:UMbw23SQkLxRJu41Bxktkxuo message:1692151076605
    ########################################################################################################################################################
    pattern = re.compile(r"\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)")


def process_log_line(line):
    
    """
    This function processes a log line and extracts relevant details.
    """

    # Regular expression for finding transaction ID and priority
    #transaction_pattern = r"transaction:([^ ]*)"
    transaction_pattern = r"(transaction:)([^ ]*)"
    #transaction_pattern = r"(transaction:|event:)([^ ]*)"
    priority_pattern = r"pri:(\d+)"

    # Parse the log line
    match = pattern.match(line)
    ## here #### match = re.match(pattern, line)

    if match:
        # Extract details
        details = match.groupdict()

        # Find transaction ID if present
        #transaction_match = re.search(transaction_pattern, details['details'])
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

        # Convert timestamp to datetime object
        # details['timestamp'] = datetime.strptime(details['timestamp'], "%Y/%m/%d %H:%M:%S")

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

    # Inicialización del diccionario de transacciones con valores predeterminados
    transactions = defaultdict(lambda: {
        'date_min': None,
        'date_max': None,
        'priority': -1,  # por defecto -1 si no hay prioridad
        'first_action': None,
        'last_action': None,
        'first_subcomponent': None,
        'last_subcomponent': None,
        'date_in_collector': None,
        'mnewtrans': None,
        'countSend': 0,
        'Duration': None,
        'duration_limsp': None
    })

    for detail in log_file_generator(file_path):
        transaction_id = detail['transaction_id']
        timestamp = detail['timestamp']
        action = detail['action']
        subcomponent = detail['subcomponent']
        priority = detail.get('priority', -1)  # Usar -1 si la prioridad es None
        mtransaction_id = detail.get('Mtransaction_id')

        #verifica si la clave existe y, si no, la establece con el valor por defecto proporcionado
        transaction = transactions.setdefault(transaction_id, {
            'date_min': timestamp,
            'date_max': timestamp,
            'first_action': action,
            'last_action': action,
            'first_subcomponent': subcomponent,
            'last_subcomponent': subcomponent,
            'priority': priority,
            'date_in_collector': None,
            'mnewtrans': mtransaction_id,
            'countSend': 0
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

        # Actualizar la prioridad, mnewtrans y contar las acciones SEND
        transaction['priority'] = priority

        if mtransaction_id is not None:
            transaction['mnewtrans'] = mtransaction_id

        # Establecer 'date_in_collector' si las condiciones coinciden
        if action == 'OUT' and subcomponent == 'FailOverManager':
            transaction['date_in_collector'] = timestamp
            

    write_result (transactions) 
                 
              
def write_result(transactions):
    # Convert the dictionary to a DataFrame
    transactions_df = pd.DataFrame.from_dict(transactions, orient='index')    
    transactions_df.index.name = 'Transaction ID'

    # Verifica y convierte 'date_in_collector' a datetime, maneja la ausencia de la columna
    #if 'date_in_collector' in transactions_df.columns:
    transactions_df['date_in_collector'] = pd.to_datetime(transactions_df['date_in_collector'], errors='coerce')

    # Calculate duration in seconds
    transactions_df['Duration'] = (transactions_df['date_max'] - transactions_df['date_min']).dt.total_seconds()

    # Calcula 'duration_limsp' donde 'date_in_collector' es válido
    mask_valid_date = transactions_df['date_in_collector'].notnull()    
    transactions_df['duration_limsp'] = float('nan')  # Inicializa con NaN
    transactions_df.loc[mask_valid_date, 'duration_limsp'] = (transactions_df['date_in_collector'] - transactions_df['date_min']).dt.total_seconds()

    # Replace NaN priorities with -1
    transactions_df['priority'] = transactions_df['priority'].fillna(-1)

    # Rearrange columns, asegurándose de que todas las columnas necesarias existan
    columns_order = [
        'date_min', 'date_max', 'priority', 'first_action', 'last_action',
        'first_subcomponent', 'last_subcomponent', 'Duration', 'mnewtrans',
        'countSend', 'date_in_collector', 'duration_limsp'
    ]
    transactions_df = transactions_df[columns_order]

    # Append or write to the CSV file
    write_mode = 'a' if countFiles > 0 else 'w'
    header = countFiles == 0
    transactions_df.to_csv(config['OutputFilePath'], mode=write_mode, header=header, index=True)
    logger_write.info(f"CountFile: {countFiles}. Records: {transactions_df.shape[0]}")


def process_log_files(directory_path, file_pattern):
    """
    This function processes all log files in a directory and its subdirectories that match a given pattern.
    """
    global countFiles
    
    logger_principal.info('Starting processing log Files...')
    # Get the directory path
    directory_path = Path(directory_path)
    
    # Get a list of all files that match the patter
    matching_files = list(directory_path.rglob(file_pattern))
    
    # Check if no files were found
    if not matching_files:
        logging.error('No files found. Terminating the script.')
        exit(1)
    
    logger_principal.info(f'File search results: {len(matching_files)} files...')
    # Iterate over each file in the directory and its subdirectories
    for file_path in directory_path.rglob(file_pattern):
        # Process the log file
        process_transactions(file_path)       
        countFiles+=1
        if countFiles % 5 == 0:
            logger_principal.info(f'Ha terminado de procesar {countFiles} archivos...')
        
config = load_config('./config/config.ini')
logger_principal, logger_files, logger_write = setup_logging()
compile_regular_expresion()



countFiles = 0

# Process all log files in the given directory and its subdirectories that match the given pattern
# Call the function with your directory path and file pattern
#process_log_files("/mnt/NAS/BANORTE/259_LATSUP-2959_Monitoreo upselling 300TPS entorno Producción/2023-08-04 01 L01/F1", "*act*.log")
#ofi
process_log_files(config['InputPath'], config['FilePattern'] )

#process_log_files("../data/", "escenario4*.log")
#home
#process_log_files("/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/logsPrueba/", "logActDR.log")

logger_principal.info('Processing completed...')

logger_principal.info('Saving results...')
archivoResultante = config['OutputFilePath']
# Save the DataFrame to a CSV file
#ofi
#all_transactions_df.to_csv("../output/result_prueba20112023_escenario4.csv")
#home3
#all_transactions_df.to_csv("/Users/colombia-01/Library/CloudStorage/OneDrive-LatiniaInteractiveBusiness,S.A/Jimmy/utiles/Python/piase/output/result_prueba04122023_1.csv")
#all_transactions_df.to_csv("/home/scriptPython/Ver_2/piase/output/result_Banorte_02102023_F1.csv")
logger_principal.info(f'Result stored {archivoResultante}')