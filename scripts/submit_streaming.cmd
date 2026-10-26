
@echo off
setlocal

set "PROJECT_DIR=%~dp0.."
set "PYTHON_SCRIPT=%PROJECT_DIR%\stream_kafka_to_hdfs.py"
set "CONTAINER_SCRIPT=/opt/spark/work-dir/stream_kafka_to_hdfs.py"
set "KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"

if not exist "%PYTHON_SCRIPT%" (
    echo ERREUR : script Python introuvable :
    echo %PYTHON_SCRIPT%
    exit /b 1
)

echo Copie du script dans le conteneur Spark...
docker cp "%PYTHON_SCRIPT%" nid_spark_master:%CONTAINER_SCRIPT%
if errorlevel 1 exit /b 1

echo Soumission du job Spark...
docker exec nid_spark_master /opt/spark/bin/spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode client ^
    --packages %KAFKA_PACKAGE% ^
    %CONTAINER_SCRIPT%

endlocal
