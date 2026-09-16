@echo off
title Kafka Topic Tool
set "JAVA_HOME=C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
set "PATH=%JAVA_HOME%\bin;%PATH%"

cd /d "D:\_SPKT\Year_4\Analyze_Big_Data\kafka"
echo Creating topic energy_raw...
.\bin\windows\kafka-topics.bat --create --topic energy_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
echo.
echo Current topics:
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
pause
