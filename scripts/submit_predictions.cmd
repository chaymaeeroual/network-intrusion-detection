@echo off
setlocal

set "PROJECT_DIR=%~dp0.."
set "PYTHON_SCRIPT=%PROJECT_DIR%\streaming\stream_kafka_to_postgres.py"
set "MODEL_DIR=%PROJECT_DIR%\intrusion_lr_model"
set "CONTAINER_SCRIPT=/opt/spark/work-dir/stream_kafka_to_postgres.py"
set "CONTAINER_MODEL=/opt/spark/intrusion_lr_model"

set "KAFKA_PACKAGE=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7"

if not exist "%PYTHON_SCRIPT%" (
    echo ERREUR : script Python introuvable.
    exit /b 1
)

if defined MODEL_ADAPTER goto model_checked

if not exist "%MODEL_DIR%" (
    echo ERREUR : modele ML introuvable.
    exit /b 1
)

:model_checked
echo Copie du script Python dans Docker...
docker cp "%PYTHON_SCRIPT%" nid_spark_master:%CONTAINER_SCRIPT%
if errorlevel 1 exit /b 1

if defined MODEL_ADAPTER goto model_ready

echo Copie du modele temporaire dans Docker...
docker exec nid_spark_master mkdir -p %CONTAINER_MODEL%
if errorlevel 1 exit /b 1

docker cp "%MODEL_DIR%\." nid_spark_master:%CONTAINER_MODEL%
if errorlevel 1 exit /b 1

:model_ready
echo Verification du mot de passe PostgreSQL...
if not defined POSTGRES_PASSWORD (
    echo ERREUR : definir POSTGRES_PASSWORD avant le lancement.
    exit /b 1
)

echo Lancement du pipeline Spark...
docker exec -e POSTGRES_PASSWORD -e KAFKA_TOPIC -e KAFKA_BOOTSTRAP_SERVERS ^
    -e MODEL_PATH -e MODEL_MODE -e MODEL_ADAPTER -e CHECKPOINT_LOCATION ^
    -e EVENT_TIMEZONE -e POSTGRES_JDBC_URL -e POSTGRES_USER -e AVAILABLE_NOW nid_spark_master ^
    /opt/spark/bin/spark-submit ^
    --master "local[2]" ^
    --deploy-mode client ^
    --conf spark.jars.ivy=/tmp/spark-ivy-cache ^
    --packages %KAFKA_PACKAGE%,org.postgresql:postgresql:42.7.4 ^
    %CONTAINER_SCRIPT%

set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
