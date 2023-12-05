import pandas as pd
import os
import re
from datetime import datetime
from pathlib import Path
import logging
import time
import configparser

## Variables de Entorno para Ejecución del Script
config = configparser.ConfigParser()
config.read('../config/config.ini')
logPath = config['duration']['logPath']
inputPath = config['duration']['inputPath']
outputPath = config['duration']['outputPath']
readSpeedPath= config['duration']['readSpeedPath']
pattern = config['duration']['pattern']
files= config['duration']['files']


def setup_logging():

    #logging.basicConfig(filename='/Users/jimmy/Library/CloudStorage/OneDrive-LatiniaInteractiveBusiness,S.A/Jimmy/utiles/Python/piase/logs/logBAC.log', level=logging.INFO,
    #                    format='%(asctime)s - %(levelname)s - %(message)s')
    logging.basicConfig(filename=logPath, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('--- Starting script ---')

def process_log_line(line):
    """
    This function processes a log line and extracts relevant details.
    """
    # Regular expression for parsing the log line
    #pattern = r"\[(?P<timestamp>.+?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    #pattern = r"(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    ########################################################################################################################################################
    #pattern = r"\[(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\]\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    #[2021/12/15 23:12:16] TOREGISTER ServiceCenterOu transaction:ULr/+jE3vcAZNPFumuwl7s+X
    #[2021/12/15 23:12:16] OUT        ServiceCenterOu transaction:ULr/+jE3vcAZNPFumuwl7s+X pri:4
    ########################################################################################################################################################
    
    #pattern = r"(?P<timestamp>\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{3})?)\s+(?P<action>.+?)\s+(?P<subcomponent>.+?)\s+(?P<details>.+)"
    #2023/08/15 21:57:56 OUT        EventManager    event:UMbw23DMS0kIqe41BxllezcS
    #2023/08/15 21:57:56 IN         WsSerInAdpAuth  transaction:UMbw23SQkLxRJu41Bxktkxuo
    #2023/08/15 21:57:56 NEWTRANS   WsSerInAdpAuth  transaction:UMbw23SQkLxRJu41Bxktkxuo message:1692151076605
    ########################################################################################################################################################   

   
   
    # Regular expression for finding transaction ID and priority
    #transaction_pattern = r"transaction:([^ ]*)"
    transaction_pattern = r"(transaction:)([^ ]*)"
    #transaction_pattern = r"(transaction:|event:)([^ ]*)"
    priority_pattern = r"pri:(\d+)"
    
    # Parse the log line
    match = re.match(pattern, line)
    
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

def process_log_file(file_path):
    """
    This function processes a log file and extracts transaction details.
    """
    global all_transactions_df

    # Read the log file
    start_time = time.time()  # Registra el tiempo de inicio
    with open(file_path, 'r') as file:
        log_lines = file.readlines()
    end_time = time.time()  # Registra el tiempo de finalización
    
    # Calcula la velocidad de lectura en MB por segundo
    file_size_mb = len(''.join(log_lines)) / (1024 * 1024)  # Tamaño del archivo en MB
    read_speed_mb_per_sec = file_size_mb / (end_time - start_time)
    
    # Registra la velocidad de lectura en el archivo de registro o donde desees
    #print(f"File: {file_path}, Read Speed (MB/s): {read_speed_mb_per_sec}\n")
    with open(readSpeedPath, 'a') as log_file:
        log_file.write(f"File: {file_path}, Read Speed (MB/s): {read_speed_mb_per_sec}\n")
       
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
        # Get transaction details
        transaction_id = row['transaction_id']
        timestamp = row['timestamp']
        action = row['action']
        subcomponent = row['subcomponent']
        priority = row['priority']
        Mtransaction_id = row['Mtransaction_id']
        
                      
        
        #print(f"TransactionId:{transaction_id} - Action:{action} - Timestamp:{timestamp} - Mtransaction_id: {Mtransaction_id}")

        # Update transaction details
        if transaction_id not in transactions:
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
                'newtrans': False  # Whether the NEWTRANS or MNEWTRANS action has been encountered
            }                  
        else:
            # If the transaction is in the dictionary, update it
            transaction = transactions[transaction_id]
                        
            if Mtransaction_id is not None :
                transactions[Mtransaction_id] = transaction
                transactions.pop(transaction_id, None)
                transaction = transactions[Mtransaction_id]         


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
    transactions_df = transactions_df[['date_min', 'date_max', 'priority', 'first_action', 'last_action', 'first_subcomponent', 'last_subcomponent', 'Duration']]

    # Append to the main DataFrame
    all_transactions_df = pd.concat([all_transactions_df, transactions_df])

def process_log_files(directory_path, file_pattern):
    """
    This function processes all log files in a directory and its subdirectories that match a given pattern.
    """
    logging.info('Starting processing log Files...')
    # Get the directory path
    directory_path = Path(directory_path)
    
    # Get a list of all files that match the patter
    matching_files = list(directory_path.rglob(file_pattern))
    
    # Check if no files were found
    if not matching_files:
        logging.error('No files found Limsp_act*. Terminating the script.')
        exit(1)
    
    logging.info(f'File search results: {len(matching_files)} files...')
    count = 0


    # Iterate over each file in the directory and its subdirectories
    for file_path in directory_path.rglob(file_pattern):
        # Process the log file
        process_log_file(file_path)
        count+=1
        if count % 2 == 0:
            logging.info(f'Ha terminado de procesar {count} archivos...')
        

setup_logging()
# Initialize a DataFrame to store the results from all files
all_transactions_df = pd.DataFrame()

# Process all log files in the given directory and its subdirectories that match the given pattern
# Call the function with your directory path and file pattern
#process_log_files("/mnt/NAS/BANORTE/259_LATSUP-2959_Monitoreo upselling 300TPS entorno Producción/2023-08-04 01 L01/F1", "*act*.log")
#ofi
#process_log_files("/Users/jimmy/Data/OneDrive - Latinia Interactive Business, S.A/Jimmy/brrMac/logsPrueba/", "*.log")
#home
process_log_files(inputPath, files)

logging.info('Processing completed...')

logging.info('Guardando resultado...')
# Save the DataFrame to a CSV file
#ofi
#all_transactions_df.to_csv("/Users/jimmy/Library/CloudStorage/OneDrive-LatiniaInteractiveBusiness,S.A/Jimmy/utiles/Python/piase/output/result_prueba20112023.csv")
#home
all_transactions_df.to_csv(outputPath)
#all_transactions_df.to_csv("/home/scriptPython/Ver_2/piase/output/result_Banorte_02102023_F1.csv")
logging.info('Resultado almacenado')
