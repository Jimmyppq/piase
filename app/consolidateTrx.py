import configparser
import fnmatch
import logging
import sys
import traceback
from collections import defaultdict
from datetime import datetime
import os
import csv

import pandas as pd

# Inicializar trx_inots como defaultdict
trx_inots = defaultdict(list)

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


def add_record_inot(transaction_id, date_max, last_action, last_subcomponent, date_in_collector, duration, duration_limsp, node_name):
    if trx_inots[transaction_id] is None:
        trx_inots[transaction_id] = []
    trx_inots[transaction_id].append({        
        "date_max": date_max,
        "last_action": last_action,
        "last_subcomponent": last_subcomponent,
        "date_in_collector": date_in_collector,
        "Duration": duration,
        "duration_limsp": duration_limsp,
        "node_name": node_name
    })

# Función para escribir el diccionario a un archivo CSV
def write_to_csv(trx_inots, output_file, first_chunk):
    all_records = []
    for trans_id, records in trx_inots.items():
        if records is None:
            continue
        for record in records:
            record_with_index = {"Transaction ID": trans_id}
            record_with_index.update(record)
            all_records.append(record_with_index)

    # Crear el DataFrame
    df = pd.DataFrame(all_records)

    # Determinar el modo de escritura y si debe incluir el encabezado
    mode = 'w' if first_chunk else 'a'
    header = first_chunk

    # Escribir el DataFrame a un archivo CSV
    df.to_csv(output_file, mode=mode, index=False, header=header, quoting=csv.QUOTE_NONE, escapechar='\\')



def consolidate_transactions(generator, output_file_path,output_result_inot, chunk_size, logger):
    """Consolida las transacciones procesadas por el generador."""
    global trx_inots
    transactions = {}
    trx_mnewtrans = {} #diccionario para gestionar mnewTrans y newtrans cuando una mnewtrans se encuentra primero que su correspondiente trans_id
    count = 0
    block = 1
    first_chunk = True
    first_chunk_inot = True
    line_number = 0


    for row in generator:
        line_number += 1
        try:
            # Intentamos desempaquetar la fila con 15 campos
            trans_id, date_min, date_max, priority, first_action, last_action, \
            first_subcomponent, last_subcomponent, duration, mnewtrans,countSend,date_in_collecctor, \
            duration_limsp,node_name,inot = row
            # Convertir countSend a entero
            countSend = int(countSend) if countSend.isdigit() else 0  # Asegura que countSend es numérico antes de convertir
            # Manejar transacciones transformadas
            # current_value[8]: date_in_collecctor
            # current_value[9]: duration_limsp
            inot = inot.lower() == 'true'
            if not inot:        
                if mnewtrans:
                    current_value = transactions.pop(mnewtrans, None)         
                    trx_inots[trans_id]= trx_inots.pop(mnewtrans,None)        
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
                            transactions[trans_id][2] = priority
            
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
            else:    
                add_record_inot(trans_id,date_max, last_action, last_subcomponent, date_in_collecctor,duration,duration_limsp,node_name)
                            
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

                df.reset_index(inplace=True)
                df.rename(columns={'index': 'Transaction ID'}, inplace=True)                
                
                if first_chunk:
                    logger.info(f"Escribiendo bloque: {block}.")                
                    df.to_csv(output_file_path, mode='w', index=False, header=True,quoting=csv.QUOTE_NONE )
                    first_chunk = False                    
                else:
                    logger.info(f"Escribiendo bloque: {block}.")
                    df.to_csv(output_file_path, mode='a', index=False, header=False,quoting=csv.QUOTE_NONE)
                block+=1       
                
                logger.info(f"Comenzando a calcular las duraciones de transacciones inot ")
                # Procesar y calcular la duración
                cant_inot = 0
                for trans_id, records in trx_inots.items():
                    if records is None:                
                        continue
                    
                    if trans_id in transactions:
                        start_time = transactions[trans_id][0]
                        for i, record in enumerate(records):
                            cant_inot +=1
                            date_max = record["date_max"]
                            duration = (date_max - start_time).total_seconds()
                            print(f"Transaction ID: {trans_id}, Record {i}, Duration: {duration} seconds")
                            # Puedes almacenar o procesar la duración calculada según sea necesario
                logger.info(f"Termina de calcular duraciones de trx inot. Total {cant_inot} inots ")

                write_to_csv(trx_inots, output_result_inot, first_chunk_inot)
                first_chunk_inot = False  
                trx_inots.clear()
                transactions.clear()  # Limpiar el diccionario para liberar memoria
        except ValueError as e:
        # Si ocurre un error de desempaquetado, imprime un mensaje y continúa
            print(f"Error procesando la línea {line_number}: {e}")
            continue        
    # Escribir cualquier transacción restante
    
    if transactions:
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


        df.reset_index(inplace=True)
        df.rename(columns={'index': 'Transaction ID'}, inplace=True)
        
        if first_chunk:
            df.to_csv(output_file_path, mode='w', index=False, header=True)
        else:
            df.to_csv(output_file_path, mode='a', index=False, header=False)
  
    # Escribir cualquier transaccion inot restante que no haya sido escrito aún
    if trx_inots:
        # Procesar y calcular la duración
        logger.info(f"Comenzando a calcular las duraciones de transacciones inot ")
        cant_inot = 0
        for trans_id, records in trx_inots.items():            
            if records is None:                
                continue
            
            if trans_id in transactions:
                start_time = transactions[trans_id][0]
                for i, record in enumerate(records):
                    cant_inot +=1
                    date_max = record["date_max"]
                    date_max_collector = record["date_in_collector"]
                    duration = (date_max - start_time).total_seconds()                    
                    duration_limsp = (date_max_collector - start_time).total_seconds()
                    record["Duration"] = duration
                    record["duration_limsp"] = duration_limsp
        logger.info(f"Termina de calcular duraciones de trx inot. Total {cant_inot} inots ")
        write_to_csv(trx_inots, output_result_inot, first_chunk_inot)        
        trx_inots.clear()
              
            

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


def validate_write_access(output_result, logger):
    """Valida si se puede escribir un archivo en la ruta especificada."""
    try:
        # Intenta crear y escribir en el archivo especificado
        with open(output_result, 'w') as test_file:
            test_file.write("Validación de acceso de escritura.")
        
        # Elimina el contenido escrito para la validación
        os.remove(output_result)
    except Exception as e:
        # Registra el error y sale de la aplicación
        logger.error(f"Error intentando escribir el archivo en {output_result}: {e}")
        logger.error(traceback.format_exc())
        exit(1)

def write_dataconfig (logger,chunk_size,log_interval,output_result,output_result_inot, input_file):
    logger.info ("VERSION 6.3")
    logger.info (f"Input File : {input_file}")   
    logger.info (f"Chunk_size_write: {chunk_size}")
    logger.info (f"LogInterval: {log_interval}")
    logger.info (f"OutputResult: {output_result}")
    logger.info (f"OutputInots: {output_result_inot}")   


def main():
    try:
        config = load_config('./config/config.ini')
        log_files = find_files(config['InputDirectory'], config['FilePattern'])
        log_interval = int(config['LogInterval'])
        output_result = config['OutputResult']
        output_result_inot = config ['OutputInots']
        log_directory = config['LogDirectory']
        filelog = config['logName']
        chunk_size = int(config['Chunk_size_write'])

        logger = setup_logger(log_directory,filelog)
        logger.info(f"Start consolidate...")
        write_dataconfig(logger,chunk_size,log_interval,output_result,output_result_inot, config['InputDirectory'])
        validate_write_access(output_result, logger)
        for file_path in log_files:
            try:
                generator = log_file_generator(file_path, log_interval, logger)
                consolidate_transactions(generator, output_result, output_result_inot, chunk_size, logger)
            except Exception as e:
                logger.error(f"Error procesando el archivo {file_path}: {e}")
                logger.error(traceback.format_exc())
                sys.exit(1)
        
        logger.info(f"...Finish")
    except FileNotFoundError as e:
        print(f"Error cargando la configuración: Archivo no encontrado: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"Error cargando la configuración: Clave faltante en el archivo de configuración: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error inesperado: {e}")
        sys.exit(1)        

if __name__ == "__main__":
    main()
    sys.exit(0)
