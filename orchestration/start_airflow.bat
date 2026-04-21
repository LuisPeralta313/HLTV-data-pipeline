@echo off
set AIRFLOW_HOME=E:\ProyectoHLTV\orchestration\airflow_home
set AIRFLOW__CORE__DAGS_FOLDER=E:\ProyectoHLTV\orchestration\dags
set AIRFLOW__CORE__EXECUTOR=SequentialExecutor
set AIRFLOW__CORE__LOAD_EXAMPLES=False

echo Arrancando Airflow Scheduler...
start "HLTV Scheduler" cmd /k "set AIRFLOW_HOME=E:\ProyectoHLTV\orchestration\airflow_home && set AIRFLOW__CORE__DAGS_FOLDER=E:\ProyectoHLTV\orchestration\dags && set AIRFLOW__CORE__EXECUTOR=SequentialExecutor && set AIRFLOW__CORE__LOAD_EXAMPLES=False && E:\ProyectoHLTV\.venv\Scripts\airflow scheduler"

timeout /t 5 /nobreak > nul

echo Arrancando Airflow Webserver en http://localhost:8080 ...
start "HLTV Webserver" cmd /k "set AIRFLOW_HOME=E:\ProyectoHLTV\orchestration\airflow_home && set AIRFLOW__CORE__DAGS_FOLDER=E:\ProyectoHLTV\orchestration\dags && set AIRFLOW__CORE__EXECUTOR=SequentialExecutor && set AIRFLOW__CORE__LOAD_EXAMPLES=False && E:\ProyectoHLTV\.venv\Scripts\airflow webserver --port 8080"

echo.
echo Airflow iniciado. Abre http://localhost:8080
