import pandas as pd
import re
from datetime import datetime
from pathlib import Path
import logging
import time
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
    transactions = {}

    transactions_collections = defaultdict(list)

    for details in log_file_generator(file_path):
        transaction_id = details['transaction_id']
        timestamp = details['timestamp']
        action = details['action']
        subcomponent = details['subcomponent']
        priority = details['priority']
        Mtransaction_id = details['Mtransaction_id']
        mnewtrans_id = None

        # Update transaction details
        if transaction_id not in transactions:
            if Mtransaction_id is not None :
                mnewtrans_id=transaction_id
                transaction_id=Mtransaction_id
            # If the transaction is not in the dictionary, add it
            transactions[transaction_id] = {
                'date_min': timestamp,
                'date_max': timestamp,
                'priority': priority if priority != -1 else None,
                'first_action': action,
                'last_action': action,
                'first_subcomponent': subcomponent,
                'last_subcomponent': subcomponent,
                'send': True if action in ('SEND', 'REJECTED') else False, # Whether the SEND or REJECTED action has been encountered
                'newtrans': False,  # Whether the NEWTRANS
                'mnewtrans':mnewtrans_id,
                'countSend':0
            }

            '''transactions_collections[transaction_id].append({
                'date_min': timestamp,
                'date_max': timestamp,
                'priority': priority if priority != -1 else None,
                'first_action': action,
                'last_action': action,
                'first_subcomponent': subcomponent,
                'last_subcomponent': subcomponent,
                'send': True if action in ('SEND', 'REJECTED') else False, # Whether the SEND or REJECTED action has been encountered
                'newtrans': False,  # Whether the NEWTRANS
                'mnewtrans':mnewtrans_id,
                'countSend':0
            })'''

        else:
            # If the transaction is in the dictionary, update it
            transaction = transactions[transaction_id]
            if Mtransaction_id is not None :
                transactions[Mtransaction_id] = transactions[transaction_id]
                transactions[Mtransaction_id]['mnewtrans'] = transaction_id
                transactions.pop(transaction_id, None)

            # Update date_min, first_action, and first_subcomponent if NEWTRANS
            if action == 'NEWTRANS' :
                transaction['date_min'] = timestamp
                transaction['first_action'] = action
                transaction['first_action'] = action
                transaction['first_subcomponent'] = subcomponent
                transaction['newtrans'] = True

            # Update date_max, last_action, and last_subcomponent only if SEND has not been encountered
            if not transaction['send']:
                transaction['date_max'] = timestamp
                transaction['last_action'] = action
                transaction['last_subcomponent'] = subcomponent

            # Update priority if it has not been set
            if transaction['priority'] is None and priority != -1:
                transaction['priority'] = priority

            # Update SEND status
            if action in ['SEND','REJECTED']:
                transaction['send'] = True
                transaction['countSend']=transaction['countSend']+1
                
                
    write_result (transactions)              
              
def write_result (transactions):
    # Convert the dictionary to a DataFrame
    transactions_df = pd.DataFrame(transactions.values(), index=transactions.keys())    
    transactions_df.index.name = 'Transaction ID'
    transactions_df = transactions_df[transactions_df.index.notnull() & (transactions_df.index != '')]
          
    # Calculate duration in seconds
    transactions_df['Duration'] = (transactions_df['date_max'] - transactions_df['date_min']).dt.total_seconds()

    # Replace NaN priorities with -1
    transactions_df['priority'] = transactions_df['priority'].fillna(-1)

    # Rearrange columns
    transactions_df = transactions_df[['date_min', 'date_max', 'priority', 'first_action', 'last_action', 'first_subcomponent', 'last_subcomponent', 'Duration','mnewtrans','countSend']]
    
    # Append to the main DataFrame
    if countFiles > 0 :    
        transactions_df.to_csv(config['OutputFilePath'], mode='a', header=False, index=True)
        logger_write.info(f"CountFile: {countFiles}. Records: {transactions_df.shape[0]}")
    else :
        transactions_df.to_csv(config['OutputFilePath'], mode='w', header=True, index=True)
        logger_write.info(f"CountFile: {countFiles}. Records: {transactions_df.shape[0]}")

#deprecated         
def process_log_file33(file_path):
    """
    This function processes a log file and extracts transaction details.
    """


    # Read the log file
    start_time = time.time()  # Registra el tiempo de inicio
    with open(file_path, 'r') as file:
        log_lines = file.readlines()
    end_time = time.time()  # Registra el tiempo de finalización    
    # Calcula la velocidad de lectura en MB por segundo
    file_size_mb = len(''.join(log_lines)) / (1024 * 1024)  # Tamaño del archivo en MB
    read_speed_mb_per_sec = file_size_mb / (end_time - start_time)    
    # Registra la velocidad de lectura en el archivo de registro o donde desees
    logger_files.info(f"File: {file_path}, Read Speed (MB/s): {read_speed_mb_per_sec}")
       
    # Process each log line and store the results in a list
    #log_data = [process_log_line(line) for line in log_lines if process_log_line(line) is not None]
    log_data = [process_log_line(line) for line in log_lines]    
    valid_log_data = [data for data in log_data if data is not None]

    # Convert the list to a pandas DataFrame
    #df = pd.DataFrame(log_data)
    df = pd.DataFrame(valid_log_data)
    if df.empty:
        print("El DataFrame está vacío! Revise la expresión regular")
        exit()

    # Drop rows without transaction_id
    df = df.dropna(subset=['transaction_id'])

    # Sort DataFrame by timestamp
    df = df.sort_values('timestamp')
    
    # Define a dictionary to store transaction details
    transactions = {}

    # Iterate over each row in the DataFrame
    for _, row in df.iterrows():        
        transaction_id = row['transaction_id']
        timestamp = row['timestamp']
        action = row['action']
        subcomponent = row['subcomponent']
        priority = row['priority']
        Mtransaction_id = row['Mtransaction_id']
        mnewtrans_id = None
     
        # Update transaction details
        if transaction_id not in transactions:
            if Mtransaction_id is not None :
                mnewtrans_id=transaction_id
                transaction_id=Mtransaction_id
            # If the transaction is not in the dictionary, add it
            transactions[transaction_id] = {
                'date_min': timestamp,
                'date_max': timestamp,
                'priority': priority if priority != -1 else None,
                'first_action': action,
                'last_action': action,
                'first_subcomponent': subcomponent,
                'last_subcomponent': subcomponent,
                'send': True if action in ('SEND', 'REJECTED') else False, # Whether the SEND or REJECTED action has been encountered
                'newtrans': False,  # Whether the NEWTRANS
                'mnewtrans':mnewtrans_id
            }                  
        else:
            # If the transaction is in the dictionary, update it            
            transaction = transactions[transaction_id]                     
            if Mtransaction_id is not None :
                transactions[Mtransaction_id] = transactions[transaction_id]
                transactions[Mtransaction_id]['mnewtrans'] = transaction_id
                transactions.pop(transaction_id, None)

            # Update date_min, first_action, and first_subcomponent if NEWTRANS
            if action == 'NEWTRANS' :
                transaction['date_min'] = timestamp
                transaction['first_action'] = action
                transaction['first_action'] = action
                transaction['first_subcomponent'] = subcomponent
                transaction['newtrans'] = True

            # Update date_max, last_action, and last_subcomponent only if SEND has not been encountered
            if not transaction['send']:
                transaction['date_max'] = timestamp
                transaction['last_action'] = action
                transaction['last_subcomponent'] = subcomponent

            # Update priority if it has not been set
            if transaction['priority'] is None and priority != -1:
                transaction['priority'] = priority

            # Update SEND status
            if action in ['SEND','REJECTED']:  
                transaction['send'] = True
    
    # Convert the dictionary to a DataFrame
    transactions_df = pd.DataFrame(transactions.values(), index=transactions.keys())
    transactions_df.index.name = 'Transaction ID'  

    # Calculate duration in seconds
    transactions_df['Duration'] = (transactions_df['date_max'] - transactions_df['date_min']).dt.total_seconds()

    # Replace NaN priorities with -1
    transactions_df['priority'] = transactions_df['priority'].fillna(-1)

    # Rearrange columns
    transactions_df = transactions_df[['date_min', 'date_max', 'priority', 'first_action', 'last_action', 'first_subcomponent', 'last_subcomponent', 'Duration','mnewtrans']]
    

    # Append to the main DataFrame
    if countFiles > 0 :    
        transactions_df.to_csv(config['OutputFilePath'], mode='a', header=False, index=True)
        logger_write.info(f"CountFile: {countFiles}. File: {file_path}. records: {transactions_df.shape[0]}")
    else :
        transactions_df.to_csv(config['OutputFilePath'], mode='w', header=True, index=True)
        logger_write.info(f"CountFile: {countFiles}. File: {file_path}. records: {transactions_df.shape[0]}")
        
    #all_transactions_df = pd.concat([all_transactions_df, transactions_df])

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
        if countFiles % 2 == 0:
            logger_principal.info(f'Ha terminado de procesar {countFiles} archivos...')
        
config = load_config('../config/config.ini')
logger_principal, logger_files, logger_write = setup_logging()
compile_regular_expresion()

countFiles = 0

# Process all log files in the given directory and its subdirectories that match the given pattern
# Call the function with your directory path and file pattern
#process_log_files("/mnt/NAS/BANORTE/259_LATSUP-2959_Monitoreo upselling 300TPS entorno Producción/2023-08-04 01 L01/F1", "*act*.log")
#ofi
process_log_files(config['InputPath'], config['FilePattern'])
#process_log_files("../data/", "escenario4*.log")
#home
#process_log_files("/Users/colombia-01/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/logsPrueba/", "logActDR.log")

logger_principal.info('Processing completed...')

logger_principal.info('Guardando resultado...')
# Save the DataFrame to a CSV file
#ofi
#all_transactions_df.to_csv("../output/result_prueba20112023_escenario4.csv")
#home3
#all_transactions_df.to_csv("/Users/colombia-01/Library/CloudStorage/OneDrive-LatiniaInteractiveBusiness,S.A/Jimmy/utiles/Python/piase/output/result_prueba04122023_1.csv")
#all_transactions_df.to_csv("/home/scriptPython/Ver_2/piase/output/result_Banorte_02102023_F1.csv")
logger_principal.info('Resultado almacenado')