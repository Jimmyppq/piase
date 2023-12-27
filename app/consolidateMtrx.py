import configparser
import fnmatch
import logging

from datetime import datetime
import os

import pandas as pd


def setup_logger(output_directory,filelog):
    """Configura el logger para escribir en un archivo en el OutputDirectory."""
    logger = logging.getLogger('LogProcessor')
    logger.setLevel(logging.INFO)

    log_file_path = os.path.join(output_directory, filelog)
    file_handler = logging.FileHandler(log_file_path)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger

def load_config(config_file):
    """Carga la configuración desde un archivo .ini."""
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['DEFAULT']

def process_log_line(line):
    """Procesa una línea del archivo de log y retorna sus componentes."""
    components = line.strip().split(',')
    if len(components) < 10:  # Filtrar líneas incompletas o mal formadas
        return None
    
    # Formatos de fecha con y sin fracción de segundo
    date_formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]  
    
    for fmt in date_formats:
        try:
            components[1] = datetime.strptime(components[1], fmt)
            break
        except ValueError:
            pass  # Intenta con el siguiente formato si ocurre un error

    for fmt in date_formats:
        try:
            components[2] = datetime.strptime(components[2], fmt)
            break
        except ValueError:
            pass  # Intenta con el siguiente formato si ocurre un error

    return components

def log_file_generator(file_path, log_interval, logger):
    """Generador para leer y procesar archivos de log."""
    line_count = 0
    with open(file_path, 'r') as file:
        next(file)  # Saltar el encabezado
        for line in file:
            processed_line = process_log_line(line)
            if processed_line:  # Filtra líneas que no contienen un ID de transacción
                yield processed_line
                line_count += 1
                if line_count % log_interval == 0:
                    logger.info(f"Se han procesado {line_count} líneas")

def consolidate_transactions(generator, output_file_path, chunk_size, logger):
    """Consolida las transacciones procesadas por el generador."""
    transactions = {}
    count = 0
    block = 1
    first_chunk = True  

    for trans_id, date_min, date_max, priority, first_action, last_action, \
        first_subcomponent, last_subcomponent, duration, mnewtrans in generator:

        # Manejar transacciones transformadas
        if mnewtrans:
            trans_id = mnewtrans

        if trans_id not in transactions:
            transactions[trans_id] = [date_min, date_max, priority, first_action, 
                                    first_subcomponent, last_action, last_subcomponent]
        else:            
            # Comprobar y actualizar la fecha mínima y sus componentes asociados
            if first_action == 'NEWTRANS' or date_min < transactions[trans_id][0]:
                transactions[trans_id][0] = date_min
                transactions[trans_id][3] = first_action
                transactions[trans_id][4] = first_subcomponent                

            # Comprobar y actualizar la fecha máxima y sus componentes asociados
            if last_action == 'SEND' or date_max > transactions[trans_id][1]:
                transactions[trans_id][1] = date_max
                transactions[trans_id][5] = last_action
                transactions[trans_id][6] = last_subcomponent                
 
        count += 1          
        if count % chunk_size == 0:
            # Calcular la duración para cada transacción
            for trans_id, values in transactions.items():           
                duration = (values[1] - values[0]).total_seconds() 
                transactions[trans_id].append(duration)                                
                """                                        
                if len(values) < 8: 
                    duration = (values[1] - values[0]).total_seconds()
                    print(f"for trx: {trans_id}. Date min {values[0]} - Date max {values[1]} duration: {duration} \n\n")
                    transactions[trans_id].append(duration)
                """    
         
            df = pd.DataFrame.from_dict(transactions, orient='index',
                                        columns=['date_min', 'date_max', 'priority', 
                                                    'first_action', 'first_subcomponent', 
                                                    'last_action', 'last_subcomponent', 'Duration'])
            df.reset_index(inplace=True)
            df.rename(columns={'index': 'Transaction ID'}, inplace=True)                
            
            if first_chunk:
                logger.info(f"Escribiendo bloque: {block}.")                
                df.to_csv(output_file_path, mode='w', index=False, header=True)
                first_chunk = False                    
            else:
                logger.info(f"Escribiendo bloque: {block}.")
                df.to_csv(output_file_path, mode='a', index=False, header=False)
            block+=1
            transactions.clear()  # Limpiar el diccionario para liberar memoria
            
            
    # Escribir cualquier transacción restante
    
    if transactions:
        for trans_id, values in transactions.items():           
            duration = (values[1] - values[0]).total_seconds() 
            transactions[trans_id].append(duration)

        df = pd.DataFrame.from_dict(transactions, orient='index',
                                    columns=['date_min', 'date_max', 'priority', 
                                             'first_action', 'first_subcomponent', 
                                             'last_action', 'last_subcomponent', 'Duration'])
        
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'Transaction ID'}, inplace=True)
        
        if first_chunk:
            df.to_csv(output_file_path, mode='w', index=False, header=False)
        else:
            df.to_csv(output_file_path, mode='a', index=False, header=False)  
            

def find_files(directory, pattern):
    """Busca archivos en un directorio de manera recursiva que coincidan con un patrón."""
    #print(f"buscando en {directory} con el patrón {pattern}")
    file_list = []
    for root, _, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                file_list.append(filename)
    return file_list

# busca archivos y utiliza generador, útil cuando la cantidad de archivos es excesiva
# se deja por si fuera necesario utilizarla, pero tal vez no sea necesario
def find_filesGenerator(directory, pattern):    
    """Busca archivos en un directorio de manera recursiva que coincidan con un patrón."""
    for root, _, files in os.walk(directory):
        for basename in files:
            if fnmatch.fnmatch(basename, pattern):
                filename = os.path.join(root, basename)
                yield filename

def main():
    
    config = load_config('./config/config.ini')
    log_files = find_files(config['InputDirectory'], config['FilePattern'])
    log_interval = int(config['LogInterval'])
    output_result = config['OutputResult']
    log_directory = config['LogDirectory']
    filelog = config['logName']
    chunk_size = int(config['Chunk_size_write'])

    logger = setup_logger(log_directory,filelog)
    logger.info(f"Start consolidate...")

    for file_path in log_files:
        generator = log_file_generator(file_path, log_interval, logger)
        consolidate_transactions(generator,output_result,chunk_size,logger)
    
    logger.info(f"...Finish")        

if __name__ == "__main__":
    main()

