                    README.MD

###########################################################

Análisis del Cambio en el Script "durationTrxHome":

Resumen de Cambios Realizados - Diciembre 2023:

1. Uso de Configuración: Se ha introducido el uso de un archivo de configuración (config.ini) para almacenar las rutas de los archivos y otros parámetros.

2. Manejo de las Transacciones con Accion "MNEWTRANS": Se ha agregado soporte para el manejo de esta particularidad en las transacciones, agregando la nueva transaccion en la columna "Mtransaction_id".  
   Cuando se crea una fila con un valor marcado en la columna  "Mtransaction_id", se utiliza este valor para actualizar la transacción existente en lugar de crear una nueva.

3. Nueva validacion en las transacciones si la variable "action" tiene un valor "SEND" o "SEND-ERR". Esto le permite conocer el final de la transaccion al script y no tomar alguna otra. Ahora bien, si esta no tiene un estado send, solo toma la ultima accion que se pinta en los archivos.

############################################################

Cambios en "consolidate.py" Script:

1. Rutas y Configuración: Al igual que en el script "durationTrxHome", utiliza un archivo de configuración (config.ini) para gestionar las rutas y otros parámetros.

2. Manejo de Múltiples Transacciones: Introduce lógica para manejar transacciones marcadas con "SEND-ERR" en lugar de "SEND" en la función get_first_last_dates_ef(). 
   Esto garantiza un manejo más eficiente en ambos casos de acción.
