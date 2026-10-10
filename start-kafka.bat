
@echo off
setlocal
title Apache Kafka Server

REM Default installation path
if not defined KAFKA_HOME set "KAFKA_HOME=C:\kafka"

if not exist "%KAFKA_HOME%\bin\windows\kafka-server-start.bat" (
    echo ERROR: Kafka not found at %KAFKA_HOME%
    pause
    exit /b 1
)

echo ============================================================
echo   STARTING APACHE KAFKA (KRaft Mode)
echo   Kafka directory: %KAFKA_HOME%
echo ============================================================

cd /d "%KAFKA_HOME%"
call ".\bin\windows\kafka-server-start.bat" ".\config\server.properties"

pause
