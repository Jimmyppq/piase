#!/bin/sh

# Descripción:
#   Este script ejecuta uno o dos scripts de Python según el parámetro proporcionado.
#   Permite la ejecución de 'durationbyTrxHome.py', 'consolidateMtrx.py', o ambos.

# Autor: Jimmy & OpenAI by GPT
# Fecha de Creación: 28/dic/2023

# Uso:
#   ./run.sh -c         Ejecuta solo el script 'consolidate.py'.
#   ./run.sh -d         Ejecuta solo el script 'durationbyTrxHome.py'.
#   ./run.sh -all       Ejecuta ambos scripts.

# Notas:
#   Los scripts de Python se ejecutan en segundo plano y sus salidas se redirigen a archivos de log.

# Ejemplos:
#   Para ejecutar solo el script de consolidación:
#   ./run.sh -c

#   Para ejecutar ambos scripts:
#   ./run.sh -all

# Función para ejecutar durationbyTrxHome
run_duration() {
    nohup python3.11 ./app/durationbyTrxHome.py > ./logs/output_durationbyTrxHome.log 2>&1 &
}

# Función para ejecutar consolidate
run_consolidate() {
    nohup python3.11 ./app/consolidateMtrx.py > ./logs/output_consolidate.log 2>&1 &
}

# Verifica el argumento pasado al script
case "$1" in
    -c)
        run_consolidate
        ;;
    -d)
        run_duration
        ;;
    -all)
        run_duration
        run_consolidate
        ;;
    *)
        echo "Uso: $0 {-c|-d|-all}"
        exit 1
        ;;
esac
