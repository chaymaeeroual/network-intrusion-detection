
@echo off
setlocal

set "PROJECT_DIR=%~dp0.."
set "PYTHON_SCRIPT=%PROJECT_DIR%\streaming\stream_kafka_to_postgres.py"set "MODEL_DIR=%PROJECT_DIR%\intrusion_lr_model"

set "CONTAINER_SCRIPT=/opt/spark/work-dir/stream_kafka_to_postgres.py"
set "CONTAINER_MODEL=/opt/spark/intrusion_lr_model"

set "KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"

if not exist "%PYTHON_SCRIPT%" (
    echo ERREUR : script Python introuvable.
    exit /b 1
)

if not exist "%MODEL_DIR%" (
    echo ERREUR : modele ML introuvable.
    exit /b 1
)

echo Copie du script Python dans Docker...
docker cp "%PYTHON_SCRIPT%" nid_spark_master:%CONTAINER_SCRIPT%
if errorlevel 1 exit /b 1

echo Copie du modele ML dans Docker...
docker cp "%MODEL_DIR%" nid_spark_master:%CONTAINER_MODEL%
if errorlevel 1 exit /b 1

echo Verification du mot de passe PostgreSQL...
if not defined POSTGRES_PASSWORD (
    echo ERREUR : definir POSTGRES_PASSWORD avant le lancement.
    exit /b 1
)

echo Lancement du pipeline Spark...
docker exec -e POSTGRES_PASSWORD nid_spark_master ^
    /opt/spark/bin/spark-submit ^
    --master "local[2]" ^
    --deploy-mode client ^
    --packages %KAFKA_PACKAGE% ^
    --jars /opt/spark/jars/postgresql-42.7.4.jar ^
    %CONTAINER_SCRIPT%

endlocal
