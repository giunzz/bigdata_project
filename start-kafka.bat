@echo off
title Apache Kafka Server
set "JAVA_HOME=C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"

echo ============================================================
echo   STARTING APACHE KAFKA (KRaft Mode)
echo   Listening on: localhost:9092
echo ============================================================

cd /d "D:\_SPKT\Year_4\Analyze_Big_Data\kafka"
.\bin\windows\kafka-server-start.bat config\server.properties
pause
