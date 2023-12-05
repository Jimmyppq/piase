import pandas as pd
import logging
import os
import time
from datetime import datetime
import configparser

## Variables de Entorno para Ejecución del Script
config = configparser.ConfigParser()
config.read('./config/config.ini')
logPath = config['consolidate']['logPath']
inputPath = config['consolidate']['inputPath']
outputPath = config['consolidate']['outputPath']

def setup_logging():
    logging.basicConfig(filename=logPath,
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('--- Starting script ---')
    

def get_first_last_dates_ef(group):
    first_row = group.iloc[0]
    last_row = group.iloc[-1]

    first_date = first_row['date_min']
    last_date = last_row['date_max']
    first_action = first_row['first_action']
    last_action = last_row['last_action']
    first_subcomponent = first_row['first_subcomponent']
    last_subcomponent = last_row['last_subcomponent']
    first_priority = first_row['priority']

    if 'NEWTRANS' in group['first_action'].values:
        newtrans_row = group[group['first_action'] == 'NEWTRANS'].iloc[0]
        first_date = newtrans_row['date_min']
        first_action = newtrans_row['first_action']
        first_subcomponent = newtrans_row['first_subcomponent']
        first_priority = newtrans_row['priority']

    elif 'MNEWTRANS' in group['first_action'].values:
        mnewtrans_row = group[group['first_action'] == 'MNEWTRANS'].iloc[0]
        first_date = mnewtrans_row['date_min']
        first_action = mnewtrans_row['first_action']
        first_subcomponent = mnewtrans_row['first_subcomponent']
        first_priority = mnewtrans_row['priority']

    if 'SEND' in group['last_action'].values:
        last_action_row = group[group['last_action'] == 'SEND'].iloc[-1]
        last_date = last_action_row['date_max']
        last_action = last_action_row['last_action']
        last_subcomponent = last_action_row['last_subcomponent']
    elif 'SEND' in group['first_action'].values:
        last_action_row = group[group['first_action'] == 'SEND'].iloc[-1]
        last_date = last_action_row['date_min']
        last_action = last_action_row['first_action']
        last_subcomponent = last_action_row['first_subcomponent']
    elif 'REJECTED' in group['last_action'].values:
        rejected_row = group[group['last_action'] == 'REJECTED'].iloc[-1]
        last_date = rejected_row['date_max']
        last_action = rejected_row['last_action']
        last_subcomponent = rejected_row['last_subcomponent']

    duration = (last_date - first_date).total_seconds()
    
    get_first_last_dates.count += 1
    
    if get_first_last_dates.count % 50000 == 0:
        logging.info(f'Ha terminado de procesar ef {get_first_last_dates.count} transacciones...')
    
    return pd.Series({
        'date_min': first_date,
        'date_max': last_date,
        'priority': first_priority,
        'first_action': first_action,
        'first_subcomponent': first_subcomponent,
        'last_action': last_action,
        'last_subcomponent': last_subcomponent, 
        'Duration': duration
    })


def get_first_last_dates(group):
    
    #print(group)
    #print ("#######here#######")
    first_date = group.iloc[0]['date_min']
    first_action = group.iloc[0]['first_action']
    first_subcomponent = group.iloc[0]['first_subcomponent']
    first_priority = group.iloc[0]['priority']
    
    last_date = group.iloc[-1]['date_max']
    last_action = group.iloc[-1]['last_action']
    last_subcomponent = group.iloc[-1]['last_subcomponent']
    
    if 'NEWTRANS' in group['first_action'].values:
        first_action_row = group[group['first_action'] == 'NEWTRANS'].iloc[0]
        first_date = first_action_row['date_min']
        first_action = first_action_row['first_action']
        first_subcomponent = first_action_row['first_subcomponent']
        first_priority = first_action_row['priority']
        
    elif 'MNEWTRANS' in group['first_action'].values:
        first_action_row = group[group['first_action'] == 'MNEWTRANS'].iloc[0]
        first_date = first_action_row['date_min']
        first_action = first_action_row['first_action']
        first_subcomponent = first_action_row['first_subcomponent']
        first_priority = first_action_row['priority']
      

    if 'SEND' in group['last_action'].values:
        last_action_row = group[group['last_action'] == 'SEND'].iloc[-1]
        last_date = last_action_row['date_max']
        last_action = last_action_row['last_action']
        last_subcomponent = last_action_row['last_subcomponent']
    elif 'SEND' in group['first_action'].values:
        last_action_row = group[group['first_action'] == 'SEND'].iloc[-1]
        last_date = last_action_row['date_min']
        last_action = last_action_row['first_action']
        last_subcomponent = last_action_row['first_subcomponent']
    elif 'REJECTED' in group['last_action'].values:
        last_action_row = group[group['last_action'] == 'REJECTED'].iloc[-1]
        last_date = last_action_row['date_max']
        last_action = last_action_row['last_action']
        last_subcomponent = last_action_row['last_subcomponent']


    duration = (last_date - first_date).seconds
    
    get_first_last_dates.count += 1
    
    # Si el contador es múltiplo de 50000, escribe en el log
    if get_first_last_dates.count % 50000 == 0:
        logging.info(f'Ha terminado de procesar {get_first_last_dates.count} transacciones...') 

    
    return pd.Series({
        'date_min': first_date,
        'date_max': last_date,
        'priority': first_priority,
        'first_action': first_action,
        'first_subcomponent': first_subcomponent,
        'last_action': last_action,
        'last_subcomponent': last_subcomponent, 
        'Duration': duration
    })

def process_csv(input_file, output_file):
    logging.info('Starting processing...')
    #data = pd.read_csv(input_file)    
    #data['date_min'] = pd.to_datetime(data['date_min'])
    #data['date_max'] = pd.to_datetime(data['date_max'])
    blocknumber = 1
    chunk_size = 100000  # Elige un tamaño de lote adecuado
    chunks = pd.read_csv(input_file, chunksize=chunk_size)
    logging.info(f'Finished reading input file: {input_file}')    

    for chunk in chunks:
        consolidated_data = pd.DataFrame()
        chunk['date_min'] = pd.to_datetime(chunk['date_min'], format='mixed')
        chunk['date_max'] = pd.to_datetime(chunk['date_max'], format='mixed')
        data_sorted = chunk.sort_values(by=['Transaction ID', 'date_min'])
        #logging.info(f'Sorting done, blockID: {blocknumber}')
        #print ("before")
        # Obtén la hora actual, incluyendo milisegundos
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # Imprime la hora actual y el mensaje
        print(f"{current_time} before blockID: {blocknumber}")        
        consolidated_data = data_sorted.groupby('Transaction ID').apply(get_first_last_dates_ef).reset_index()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print (f"{current_time} after blockID: {blocknumber}")   
        print (f"blockID: {blocknumber}")        
        logging.info(f'Done...blockID: {blocknumber}')
        blocknumber +=1        
        # Abre el archivo en modo de apendizaje ('a') en lugar de escritura ('w')        
        with open(output_file, 'a') as f:
            # Utiliza el método to_csv, pero también pasa el objeto de archivo 'f'
            consolidated_data.to_csv(f, index=False, header=f.tell()==0)  # Añade el encabezado solo si el archivo está vacío
        
    #print(consolidated_data)
    '''
    escape_mapping = {
        '*': '\\*',
        '/': '\\/',
        '=': '\\=',
        '+': '\\+'
    }
 
    for char, replacement in escape_mapping.items():
        consolidated_data['Transaction ID'] = consolidated_data['Transaction ID'].str.replace(char, replacement)
    '''
    #consolidated_data.to_csv(output_file, index=False)
    logging.info('Processing completed.')

if __name__ == "__main__":
    setup_logging()
    # Inicializa el contador
    get_first_last_dates.count = 0
    
    input_file_path = inputPath
    output_file_path = outputPath
    if not os.path.exists(input_file_path):
        logging.error(f"El archivo {input_file_path} no existe. Terminando la ejecución.")
        exit(1)
        
    process_csv(input_file_path, output_file_path)
    logging.info('--- Script finished ---')
