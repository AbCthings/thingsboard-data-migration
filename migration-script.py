'''
@Author Alessandro Bortoletto - LINKS Foundation

@Version 0.0.4

@Date 01-07-2019

@Brief This is a collection of useful functions to perform operations with ThingsBoard DB via RESTful API or MQTT

'''

# *** Import section ***
import requests
from datetime import datetime
import time
import sys
import paho.mqtt.client as mqtt
import json
import argparse
import yaml
import logging
from tqdm import tqdm

'''
@Name getData(ip,port,user,password,deviceId,key,startTs,endTs)
@Return Returns a timeseries with the response to the HTTP RESTful request
@Parameters
    ip: ip address of target ThingsBoard
    port: port used by the target ThingsBoard
    user: username to obtain API authorization
    password: password to obtain API authorization
    deviceId: id of the device to read
    key: key of the timeseries to read
    startTs: first unix timestamp to fetch
    endTs: last unix timestamp to fetch
@Notes The function reads all the database entries associated with the device and key, with a LIMIT of 200000 and no aggregation
'''
def getData(ip,port,user,password,deviceId,key,startTs,endTs):
 
    # Define some constants 
    INTERVAL = '0'
    LIMIT = '200000'
    AGG = 'NONE'
    
    # Define the headers of the authorization request
    headersToken = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    # Define the body of the authorization request
    data = '{"username":"'+user+'", "password":"'+password+'"}'

    # Perform the POST request to obtain X-Token Authorization
    try:
        response = requests.post('https://'+ip+':'+port+'/api/auth/login', headers=headersToken, data=data)
        X_AUTH_TOKEN = response.json()['token']
        #print("Codice autorizzazione: ",X_AUTH_TOKEN)
    except Exception as e:
        print("\nAn exception occurred while trying to obtain the authorization from SOURCE Thigsboard: ", e)
        sys.exit(1)

    # Define the headers of the request
    headers = {'Accept':'application/json','X-Authorization': 'Bearer '+X_AUTH_TOKEN}

    # Perform the GET request to obtain timeseries
    try:
        r = requests.get("https://"+ip+":"+port+"/api/plugins/telemetry/DEVICE/"+deviceId+"/values/timeseries?interval="+INTERVAL+"&limit="+LIMIT+"&agg="+AGG+"&keys="+key+"&startTs="+startTs+"&endTs="+endTs,headers=headers)
        print("Request to SOURCE ThingsBoard - response code: ", r.status_code)
    except Exception as e:
        print("\nAn exception occurred while trying to obtain and print the timeseries from SOURCE Thigsboard: ", e)
        logging.error('Timeseries request failed.')
        sys.exit(1)

    # Define the timeseries to upload
    TIMESERIES = r.json()
    #print("Fetched timseries: ", TIMESERIES)
    
    logging.info('Timeseries request successful.')
	
    # Return the result of the GET request
    return TIMESERIES

'''
@Name sendData(ip,port,deviceToken,key,timeseries)
@Return None
@Parameters
    ip: ip address of target ThingsBoard
    port: port used by the target ThingsBoard
    deviceToken: token of the device to upload
    key: key of the timeseries to upload
    timeseries: actual array containing the timeseries to upload: the last element shall be the one with the OLDEST unix timestamp
@Notes This function uploads a timeseries (passed as argument) via MQTT
'''
def sendData(ip,port,deviceToken,key,timeseries):
    
    # Create MQTT client
    client = mqtt.Client()

    # Set access token
    client.username_pw_set(deviceToken)

    # Connect to ThingsBoard using default MQTT port and 60 seconds keepalive interval
    client.connect(ip, int(port), 60)
    client.loop_start()

    # Declare data format
    data = {"ts":0, "values":{key:0}}

    # Upload the timeseries with the proper timestamps and values
    try:
        
        # Send all the TIMESERIES values via MQTT, START FROM THE LAST ELEMENT SINCE IT IS THE OLDEST ONE
        for i in tqdm(range(len(timeseries[key])-1, -1, -1), desc='Uploading data to TARGET ThingsBoard'):

            value = timeseries[key][i]['value']
            ts = timeseries[key][i]['ts']

            data['ts'] = ts
            data['values'][key] = value

            # Send data to ThingsBoard via MQTT
            client.publish('v1/devices/me/telemetry', json.dumps(data), 1)
            #print("Upload timestamp: ", datetime.fromtimestamp(ts/1000), " | Raw data: ", data)

            # THE DELAY IS NECESSARY TO AVOID THE ThingsBoard "WEB_SOCKET: TOO MANY REQUESTS" ERROR
            time.sleep(0.01)

    except KeyboardInterrupt:
        print("The user manually interrputed the MQTT upload using keyboard.")
        logging.error('Timeseries upload failed.')
        pass
    else:
        print("Data successfully published via MQTT.")

    logging.info('Timeseries upload successful.')
	
    # Close the MQTT connections
    client.loop_stop()
    client.disconnect()

'''
@Name saveToFile(key,timeseries,file)
@Return None
@Parameters
    key: key of the timeseries to save on file
    timeseries: actual array containing the timeseries to save
    file: name of the file to use for saving
@Notes This function saves into a file a timeseries
'''
def saveToFile(key,timeseries,file):
    
    try:
        
        # Open the file in append mode
        file = open(file, 'a+')

        # Temporary variable to save timeseries entries
        data = {"ts":0, "value":0}
        
        # Iterate the timeseries and append to file, FROM THE LAST ELEMENT SINCE IT IS THE OLDEST ONE
        for i in tqdm(range(len(timeseries[key])-1, -1, -1), desc='Saving data to local file'):

            value = timeseries[key][i]['value']
            ts = timeseries[key][i]['ts']

            data['ts'] = ts
            data['value'] = value

            file.write(json.dumps(data) + "\n")
        
        # Close the file when finished
        file.close()

    except Exception as exception:
    
        print("An error occoured while saving the data into local text file: ", exception)
        logging.error('An error occoured while saving the data into local text file.')
        sys.exit()

'''
@Name readFromFile(key,file)
@Return timeseries
@Parameters
    key: key of the timeseries to read from file
    file: name of the file to use for reading
@Notes This function reads a timeseries from a file
'''
def readFromFile(key,file):
    
    timeseries = []
	
    try:
        
        # Open the file in append mode
        file = open(file, 'r')

        for line in file:
            timeseries.append(json.loads(line))
        
        # Close the file when finished
        file.close()
		
		# Format and return the correct timeseries
        response = {key: timeseries}
        return response

    except Exception as exception:
    
        print("An error occoured while reading the data from local text file: ", exception)
        logging.error('An error occoured while reading the data from local text file.')
        sys.exit()
		
		
# ******* THE SCRIPT'S OPERATIONS BEGIN HERE *******

'''
@Notes This is the argument parser of the script
'''
# This sections parses the input arguments and saves them into constant variables
parser = argparse.ArgumentParser(description="This script performs a data migration between two different instances of ThingsBoard server.")

parser.add_argument("-c", action="store", dest="configuration", type=str, default="./migrationConf.yml",
                    help="specify path of the YAML configuration file (default './migrationConf.yml')")
parser.add_argument("-m", action="store", dest="mode", type=str, default="both",
                    help="specify operating mode of the script. 'fetch' --> save data in local text file | 'send' --> send data from local text file | 'both' --> fetch and send data (default 'both')")
parser.add_argument("-i", action="store", dest="initialTs", type=str, default="",
                    help="specify initial UNIX timestamp", required=True)
parser.add_argument("-f", action="store", dest="finalTs", type=str, default="",
                    help="specify final UNIX timestamp", required=True)
parser.add_argument("-s", action="store", dest="sourceDeviceId", type=str, default="None",
                    help="specify the ID of the source device", required=True)
parser.add_argument("-t", action="store", dest="targetDeviceToken", type=str, default="None",
                    help="specify the token of the target device", required=True)
parser.add_argument("-k", action="store", dest="timeseriesKey", type=str, default="None",
                    help="specify the key (name) of the timeseries to migrate", required=True)

args = parser.parse_args()

CONFIG_FILE = args.configuration
MODE = args.mode
STARTTS = args.initialTs
ENDTS = args.finalTs
SOURCE_TB_DEVICE_ID = args.sourceDeviceId
TARGET_TB_DEVICE_TOKEN = args.targetDeviceToken
TIMESERIES_KEY = args.timeseriesKey

'''
@Notes This is the config file reader of the script

'''
try:

    with open(CONFIG_FILE, 'r') as ymlfile:
        cfg = yaml.load(ymlfile, Loader=yaml.FullLoader)
    
    # Save here all the configuration variables
    SOURCE_TB_ADDRESS = cfg['source']['host']
    SOURCE_TB_PORT = cfg['source']['port']
    SOURCE_TB_USER = cfg['source']['user']
    SOURCE_TB_PASSWORD = cfg['source']['password']
    TARGET_TB_ADDRESS = cfg['target']['host']
    TARGET_TB_PORT = cfg['target']['port']
    LOG_FILE = cfg['log']['file']
    DB_FILE = cfg['db']['file']
	
    ymlfile.close()
    
except Exception as exception:
    
    print("An error occoured while reading the configuration file:", exception)
    print("Is it configured correctly?")
    sys.exit()
	
# The port of source/target can be empty
if SOURCE_TB_PORT == None:
	SOURCE_TB_PORT = ""
if TARGET_TB_PORT == None:
	TARGET_TB_PORT = ""
	
# Modify the local database text file name --> This makes it unique for any source device and key
DB_FILE += "-" + SOURCE_TB_DEVICE_ID + "-" + TIMESERIES_KEY + ".db"

'''
@Notes Here i open the log file. Levels of logging: DEBUG INFO WARNING ERROR CRITICAL
'''
# Open and configure logger
logging.basicConfig(filename=LOG_FILE, filemode='a+', format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%d-%b-%y %H:%M:%S')
# Set minimum log level to "info"
logging.getLogger().setLevel(logging.INFO)
# Log something
logging.info('The script has been correctly initialized.')

'''
@Notes This is the core of the algorithm, which uses a different operating mode depending on what is passed to the script as an argument
'''
# Both mode
if MODE == "both":
	logging.info('Operating mode: both')
	
	logging.info('Started to fetch data')
	data = getData(SOURCE_TB_ADDRESS,SOURCE_TB_PORT,SOURCE_TB_USER,SOURCE_TB_PASSWORD,SOURCE_TB_DEVICE_ID,TIMESERIES_KEY,STARTTS,ENDTS)
	logging.info('Finished to fetch data')
	
	logging.info('Started to send data')
	sendData(TARGET_TB_ADDRESS,TARGET_TB_PORT,TARGET_TB_DEVICE_TOKEN,TIMESERIES_KEY,data)
	logging.info('Finished to send data')
	
# Fetch mode
if MODE == "fetch":
	logging.info('Operating mode: fetch')
	
	logging.info('Started to fetch data')
	data = getData(SOURCE_TB_ADDRESS,SOURCE_TB_PORT,SOURCE_TB_USER,SOURCE_TB_PASSWORD,SOURCE_TB_DEVICE_ID,TIMESERIES_KEY,STARTTS,ENDTS)
	logging.info('Finished to fetch data')
	
	logging.info('Started to save data')
	saveToFile(TIMESERIES_KEY,data,DB_FILE)
	logging.info('Finished to save data')

# Send mode	
if MODE == "send":
	logging.info('Operating mode: send')
	
	logging.info('Started reading data from file')
	data = readFromFile(TIMESERIES_KEY,DB_FILE)
	logging.info('Finished reading data from file')
	
	logging.info('Started to send data')
	sendData(TARGET_TB_ADDRESS,TARGET_TB_PORT,TARGET_TB_DEVICE_TOKEN,TIMESERIES_KEY,data)
	logging.info('Finished to send data')
	
'''
@Notes Close and exit
'''
logging.info('Execution finished.')
print('Execution finished.')

