import pandas as pd
import logging
import os
import time


def setup_logging():
    logging.basicConfig(filename='/Users/jimmy/Library/CloudStorage/OneDrive-LatiniaInteractiveBusiness,S.A/Jimmy/utiles/Python/piase/logs/consolidation21102023.log',
                        level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('--- Starting script ---')
    


def get_first_last_dates(group):


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
    chunk_size = 5  # Elige un tamaño de lote adecuado
    chunks = pd.read_csv(input_file, chunksize=chunk_size)
    for chunk in chunks:
        chunk['date_min'] = pd.to_datetime(chunk['date_min'], format='mixed')
        chunk['date_max'] = pd.to_datetime(chunk['date_max'], format='mixed')
        data_sorted = chunk.sort_values(by=['Transaction ID', 'date_min'])
        logging.info(f'Sorting done, blockID: {blocknumber}')        
        consolidated_data = data_sorted.groupby('Transaction ID').apply(get_first_last_dates).reset_index()            
        logging.info(f'Groupby done. blockID: {blocknumber}')
        blocknumber +=1        
        #consolidated_data.to_csv(output_file, index=False)
        # Abre el archivo en modo de apendizaje ('a') en lugar de escritura ('w')
        with open(output_file, 'a') as f:
            # Utiliza el método to_csv, pero también pasa el objeto de archivo 'f'
            consolidated_data.to_csv(f, index=False, header=f.tell()==0)  # Añade el encabezado solo si el archivo está vacío
            
    logging.info(f'Finished reading input file: {input_file}')    
        
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
    
    input_file_path = "/Users/jimmy/Library/CloudStorage/OneDrive-LatiniaInteractiveBusiness,S.A/Jimmy/utiles/Python/piase/output/result_test21102023.csv"
    output_file_path = "/Users/jimmy/Library/CloudStorage/OneDrive-LatiniaInteractiveBusiness,S.A/Jimmy/utiles/Python/piase/output/consolidate_test_21102023.csv"
    if not os.path.exists(input_file_path):
        logging.error(f"El archivo {input_file_path} no existe. Terminando la ejecución.")
        exit(1)
        
    process_csv(input_file_path, output_file_path)
    logging.info('--- Script finished ---')