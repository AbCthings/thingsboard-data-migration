# thingsboard-data-migration
This tool performs data migration between two different instances of ThingsBoard, regardless of the technology used for the database.
<br>
It also allows the transfer of data between two different devices of the same ThingsBoard instance.
<br>
The tool exploits the native ThingsBoard HTTP APIs to retrive data, and the ThingsBoard MQTT broker to send data.

# How to use
usage: migration-script.py [-h] [-c CONFIGURATION] [-m MODE] -i 
       INITIALTS -f FINALTS -s SOURCEDEVICEID -t  
       TARGETDEVICETOKEN -k TIMESERIESKEY

# Help & examples
python migration-script.py --help

Command example:
python migration-script.py -m both -c ./migrationConf.yml -i 1546361494000 -f 1578415926000 -s dc872480-85e5-11e9-acf5-fb7ea3e0493d -t phhQnVa4nSKjyJ1QMwtx -k data_key

# Operating modes
You can perform three different operations with this script:
1) Fetch data via HTTP RESTful API (from source ThingsBoard instance) <i>and</i> send via MQTT (to target ThingsBoard instance) --> use argument "-m <b>both</b>", default behavior
2) Fetch data via HTTP RESTful API (from source ThingsBoard instance) and save in local file --> use argument "-m <b>fetch</b>"
3) Fetch data from local file and send via MQTT (to target ThingsBoard instance) --> use argument "-m <b>send</b>"

*** IMPORTANT ***
<br>
You shall always pass ALL the arguments, even if not used within the chosen operating mode. Next version of this script will include an automated argument check to allow the user specify only the strictly necessary ones.
