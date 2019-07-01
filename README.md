# thingsboard-data-migration
This script performs data migration between two different instances of ThingsBoard, regardless of the technology used for the database.

# How to use
usage: migration-script.py [-h] [-c CONFIGURATION] [-m MODE] -i 
       INITIALTS -f FINALTS -s SOURCEDEVICEID -t  
       TARGETDEVICETOKEN -k TIMESERIESKEY

# Help
python migration-script.py --help

# Modes
You can perform three different operations with this script:
1) Fetch data via HTTP RESTful API (from source ThingsBoard instance) and send via MQTT (to target ThingsBoard instance) --> use argument "-m <b>both</b>", default behavior
2) Fetch data via HTTP RESTful API (from source ThingsBoard instance) and save in local file --> use argument "-m <b>fetch</b>"
3) Fetch data from local file and send via MQTT (to target ThingsBoard instance) --> use argument "-m <b>send</b>"
