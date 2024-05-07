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
    return config['CONSOLIDATE']

def process_log_line(line):
    """Procesa una línea del archivo de log y retorna sus componentes."""
    components = line.strip().split(',')
    if len(components) < 11:  # Filtrar líneas incompletas o mal formadas
        return None
    
    # Formatos de fecha con y sin fracción de segundo
    date_formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"] 

    #date_min
    for fmt in date_formats:
        try:
            components[1] = datetime.strptime(components[1], fmt)
            break
        except ValueError:
            pass  # Intenta con el siguiente formato si ocurre un error

    #date_max
    for fmt in date_formats:
        try:
            components[2] = datetime.strptime(components[2], fmt)
            break
        except ValueError:
            pass  # Intenta con el siguiente formato si ocurre un error
    
    #date_in_collector
    if components[11].strip():  # Verifica si hay un valor no vacío
        for fmt in date_formats:
            try:
                components[11] = datetime.strptime(components[11], fmt)
                break
            except ValueError:
                pass  # Intenta con el siguiente formato si ocurre un error
    else:
        components[11] = None  # Establece como None si está vacío o contiene solo espacios

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

def consolidate_trx(generator, output_file_path, chunk_size, logger):
    """Consolida las transacciones procesadas por el generador."""
    transactions = {}
    count = 0
    block = 1
    first_chunk = True  

    def flush_to_csv():
        df = pd.DataFrame.from_dict(transactions, orient='index')
        df.reset_index(inplace=True)

        df['duration'] = (df['date_max'] - df['date_min']).dt.total_seconds()
        df['duration_limsp'] = (df['date_min'] - df['date_in_collector']).dt.total_seconds()

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



    for trans_id, date_min, date_max, priority_str, first_action, last_action, \
        first_subcomponent, last_subcomponent, _, _, countSend_str, date_in_collector_str, duration_limsp_str in generator:


        # Convertir los valores a los tipos adecuados
        #date_min = datetime.strptime(date_min_str, '%Y-%m-%d %H:%M:%S')
        #date_max = datetime.strptime(date_max_str, '%Y-%m-%d %H:%M:%S')
        priority = int(priority_str)
        countSend = int(countSend_str) if countSend_str else 0
        date_in_collector = datetime.strptime(date_in_collector_str, '%Y-%m-%d %H:%M:%S') if date_in_collector_str else None
        duration_limsp = float(duration_limsp_str) if duration_limsp_str else 0.0

        # Actualizar o inicializar la entrada de transacción
        if trans_id not in transactions:
            transactions[trans_id] = {
                'date_min': date_min,
                'date_max': date_max,
                'priority': priority,
                'first_action': first_action,
                'last_action': last_action,
                'first_subcomponent': first_subcomponent,
                'last_subcomponent': last_subcomponent,
                'countSend': countSend,
                'date_in_collector': date_in_collector,
                'duration_limsp': duration_limsp
            }
        else: 
            trans = transactions[trans_id]
            if first_action == "NEWTRANS":
                trans['date_min'] = date_min
                trans['first_action'] = first_action
                trans['first_subcomponent'] = first_subcomponent
            elif trans['first_action'] != "NEWTRANS" and date_min < trans['date_min']:
                trans['date_min'] = date_min
                trans['first_action'] = first_action
                trans['first_subcomponent'] = first_subcomponent

            if last_action == "SEND":
                trans['date_max'] = date_max
                trans['last_action'] = last_action
                trans['last_subcomponent'] = last_subcomponent
            elif trans['last_action'] != "SEND" and date_max > trans['date_max']:
                trans['date_max'] = date_max
                trans['last_action'] = last_action
                trans['last_subcomponent'] = last_subcomponent

            trans['priority'] = max(trans['priority'], priority)
            trans['countSend'] += countSend
            if date_in_collector:
                trans['date_in_collector'] = min(trans['date_in_collector'], date_in_collector)
                trans['duration_limsp'] = duration_limsp
     
        count += 1          
        if count % chunk_size == 0:
            flush_to_csv()


            
    # Escribir cualquier transacción restante
    
    if transactions:
        for trans_id, values in transactions.items():           
            duration = (values[1] - values[0]).total_seconds() 
            transactions[trans_id].append(duration)
           # duration_limsp = (values[9] - values[0]).total_seconds()
            print(f'trans_id: {trans_id} -- values 8 {values[8]} duration limsp: {duration_limsp}')

        df = pd.DataFrame.from_dict(transactions, orient='index',
                                    columns=['date_min', 'date_max', 'priority', 
                                             'first_action', 'first_subcomponent', 
                                             'last_action', 'last_subcomponent', 'countSend','date_in_collecctor','duration_limsp','Duration'])
        
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'Transaction ID'}, inplace=True)
        
        if first_chunk:
            df.to_csv(output_file_path, mode='w', index=False, header=True)
        else:
            df.to_csv(output_file_path, mode='a', index=False, header=False)


def consolidate_transactions(generator, output_file_path, chunk_size, logger):
    """Consolida las transacciones procesadas por el generador."""
    transactions = {}
    trx_mnewtrans = {} #diccionario para gestionar mnewTrans y newtrans cuando una mnewtrans se encuentra primero que su correspondiente trans_id
    count = 0
    block = 1
    first_chunk = True  

    for trans_id, date_min, date_max, priority, first_action, last_action, \
        first_subcomponent, last_subcomponent, duration, mnewtrans,countSend,date_in_collecctor,duration_limsp,node_name  in generator:
        # Convertir countSend a entero
        countSend = int(countSend) if countSend.isdigit() else 0  # Asegura que countSend es numérico antes de convertir
        # Manejar transacciones transformadas
        # current_value[8]: date_in_collecctor
        # current_value[9]: duration_limsp        
        if mnewtrans:
            current_value = transactions.pop(mnewtrans, None)            
            if current_value is not None:
                if trans_id in transactions:
                    if current_value[3] == 'NEWTRANS' or current_value[0] < transactions[trans_id][0]:
                        transactions[trans_id][0] = current_value[0] #date_min
                        transactions[trans_id][3] = current_value[3] #first_action
                        transactions[trans_id][4] = current_value[4] #first_subcomponent                        
                    
                    if current_value[5] == 'SEND' or current_value[1]>transactions[trans_id][1]:
                        transactions[trans_id][1] = current_value[1] #date_max
                        transactions[trans_id][5] = current_value[5] #last_action
                        transactions[trans_id][6] = current_value[6] #last_subcomponent
                        transactions[trans_id][8] = current_value [8] #date_in_collector
                        transactions[trans_id][7] +=1    
                else:                    
                    transactions[trans_id] = current_value
            else:               
               trx_mnewtrans[mnewtrans]= trans_id
        else:
            if trans_id in trx_mnewtrans:
                trx=trx_mnewtrans.pop(trans_id,None)
                
                current_value = transactions.pop(trx, None) 
                if current_value is not None:
                    transactions[trans_id] = current_value
    
        if trans_id not in transactions:           
            transactions[trans_id] = [date_min, date_max, priority, first_action, 
                                    first_subcomponent, last_action, last_subcomponent, countSend,date_in_collecctor, node_name]           
        else:          
            # Comprobar y actualizar la fecha mínima y sus componentes asociados            
            if transactions[trans_id][3] != 'NEWTRANS':
                if date_min < transactions[trans_id][0] or first_action == 'NEWTRANS' :
                    transactions[trans_id][0] = date_min
                    transactions[trans_id][3] = first_action
                    transactions[trans_id][4] = first_subcomponent
            
            if transactions[trans_id][5] != 'SEND':
                # Comprobar y actualizar la fecha máxima y sus componentes asociados
                if date_max > transactions[trans_id][1] or last_action == 'SEND':
                    transactions[trans_id][1] = date_max
                    transactions[trans_id][5] = last_action
                    transactions[trans_id][6] = last_subcomponent                    
                    transactions[trans_id][7] +=1  
                               
            if date_in_collecctor is not None :
                transactions[trans_id][8] = date_in_collecctor

             

        count += 1          
        if count % chunk_size == 0:
            # Calcular la duración para cada transacción
            
            for trans_id, values in transactions.items():           
                duration = (values[1] - values[0]).total_seconds() 
                duration_limsp = (values[8] - values[0]).total_seconds() if values[8] else 0
                transactions[trans_id].append(duration)
                transactions[trans_id].append(duration_limsp)


            df = pd.DataFrame.from_dict(transactions, orient='index',
                            columns=['date_min', 'date_max', 'priority', 'first_action', 
                                     'first_subcomponent', 'last_action', 'last_subcomponent', 
                                     'countSend', 'date_in_collector', 'node_name', 'Duration', 'duration_limsp'])

            # Reorganiza las columnas para mover 'node_name' al final
            column_order = [col for col in df.columns if col != 'node_name'] + ['node_name']
            df = df[column_order]


            '''df = pd.DataFrame.from_dict(transactions, orient='index',
                                        columns=['date_min', 'date_max', 'priority', 
                                                    'first_action', 'first_subcomponent', 
                                                    'last_action', 'last_subcomponent', 'countSend', 'date_in_collecctor','Duration','duration_limsp'])
            '''
            
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
            duration_limsp = (values[8] - values[0]).total_seconds() if values[8] else 0
            
            #print(f"date_in_collecctor  {values['date_in_collecctor']} duration limsp: {duration_limsp}")
            #print (f"transID: {trans_id} values[8] {values[8]} - values[1] {values[1]}. Duration_limsp:  {duration_limsp}")
            #transactions[trans_id].append(duration_limsp)            
            transactions[trans_id].append(duration)
            transactions[trans_id].append(duration_limsp)
            


        df = pd.DataFrame.from_dict(transactions, orient='index',
                        columns=['date_min', 'date_max', 'priority', 'first_action', 
                                    'first_subcomponent', 'last_action', 'last_subcomponent', 
                                    'countSend', 'date_in_collector', 'node_name', 'Duration', 'duration_limsp'])

        # Reorganiza las columnas para mover 'node_name' al final
        column_order = [col for col in df.columns if col != 'node_name'] + ['node_name']
        df = df[column_order]


        '''df = pd.DataFrame.from_dict(transactions, orient='index',
                                    columns=['date_min', 'date_max', 'priority', 
                                             'first_action', 'first_subcomponent', 
                                             'last_action', 'last_subcomponent', 'countSend','date_in_collecctor','Duration','duration_limsp'])
        '''
        
        df.reset_index(inplace=True)
        df.rename(columns={'index': 'Transaction ID'}, inplace=True)
        
        if first_chunk:
            df.to_csv(output_file_path, mode='w', index=False, header=True)
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

