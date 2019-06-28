# thingsboard-data-migration
This repository contains source code to perform data migration between two different instances of ThingsBoard, regardless of the technology used for the database.

# How to use
usage: migration-script-v0.0.3-latest.py [-h] [-c CONFIGURATION] [-m MODE] -i 
       INITIALTS -f FINALTS -s SOURCEDEVICEID -t  
       TARGETDEVICETOKEN -k TIMESERIESKEY

# Help
python migration-script-v0.0.3.py --help

# Modes
You can perform three operations with this script
1) Fetch data via HTTP API (from source ThingsBoard instance) and send via MQTT (to target ThingsBoard instance) --> use argument "-m both", default behavior
2) Fetch data via HTTP API (from source ThingsBoard instance) and save in local file --> use argument "-m fetch"
3) Fetch data from local file and send via MQTT (to target ThingsBoard instance) --> use argument "-m send"
