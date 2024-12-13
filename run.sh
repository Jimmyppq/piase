#!/bin/sh

# Descripción:
#   Este script ejecuta uno o dos scripts de Python según el parámetro proporcionado.
#   Permite la ejecución de 'durationbyTrxHome.py', 'consolidateMtrx.py', o ambos.

# Autor: Jimmy & OpenAI by GPT
# Fecha de Creación: 28/dic/2023

# Uso:
#   ./run.sh -consolidate    Ejecuta solo el script 'consolidate.py'.
#   ./run.sh -duration       Ejecuta solo el script 'durationbyTrxHome.py'.
#   ./run.sh -all            Ejecuta ambos scripts.

# Notas:
#   Los scripts de Python se ejecutan en segundo plano y sus salidas se redirigen a archivos de log.

# Ejemplos:
#   Para ejecutar solo el script de consolidación:
#   ./run.sh -consolidate

#   Para ejecutar ambos scripts:
#   ./run.sh -all

# Función para ejecutar durationbyTrxHome
run_duration() {
    nohup python3.11 ./app/durationtrxlimsp.py > ./logs/output_durationbyTrx.log 2>&1 &
}

# Función para ejecutar consolidate

run_consolidate() {
    nohup python3.11 ./app/consolidateTrx.py > ./logs/output_consolidate.log 2>&1 &
}

run_qosreadfiles(){
	nohup python3.6 ./app/BinaryTransactionReader.py > ./logs/BinaryTransactionReader.log 2>&1 &
}

run_qosprocess(){
	nohup python3.6 ./app/durationtrxlimsp_class.py > ./logs/qosprocess.log 2>&1 &
}


run_qosreviewincomplete(){
    nohup python3.6 ./app/ReviewIncomplete.py > ./logs/ReviewIncomplete.log 2>&1 &
}

# Verifica el argumento pasado al script
case "$1" in
    -c)
        run_consolidate
        ;;
    -d)
        run_duration
        ;;
    -p)
        run_qosreviewincomplete
        ;;
    -r)
    	run_qosreadfiles
    	;;
    -s)
        run_qosprocess
        ;;

    -all)
        run_duration
        run_consolidate
        ;;
    *)
        echo "Uso: $0 {-c|-d|-s|-all}"
        exit 1
        ;;
esac

