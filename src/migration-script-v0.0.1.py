'''
@Author Alessandro Bortoletto - LINKS Foundation
@Date 03-06-2019
@Version 0.0.1
@Brief This script transfers a device timeseries between two different instances of Thingsboard, regardless of the technology used for the database
@Detail The logical flow of the application is: 1) Get the specific timeseries from the SOURCE instance of Thingsboard, via HTTP API 2) Upload the specific timeseries to the TARGET instance of Thingsboard, via MQTT
'''

# *** Import section ***
import requests
from datetime import datetime
import time
import sys
import paho.mqtt.client as mqtt
import json

# *** GET section, via HTTP API ***
def getData():
    # Define the headers of the authorization request
    headersToken = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    # Define the body of the authorization request
    data = '{"username":"'+SOURCE_TB_USER+'", "password":"'+SOURCE_TB_PASSWORD+'"}'

    # Perform the POST request to obtain X-Token Authorization
    try:
        response = requests.post('http://'+SOURCE_TB_ADDRESS+':'+SOURCE_TB_PORT+'/api/auth/login', headers=headersToken, data=data)
        X_AUTH_TOKEN = response.json()['token']
        print("Codice autorizzazione: ",X_AUTH_TOKEN)
    except Exception as e:
        print("\nAn exception occurred while trying to obtain the authorization from SOURCE Thigsboard: ", e)
        sys.exit(1)

    # Define the headers of the request
    headers = {'Accept':'application/json','X-Authorization': 'Bearer '+X_AUTH_TOKEN}

    # Perform the GET request to obtain timeseries
    try:
        r = requests.get("http://"+SOURCE_TB_ADDRESS+":"+SOURCE_TB_PORT+"/api/plugins/telemetry/DEVICE/"+SOURCE_TB_DEVICE_ID+"/values/timeseries?interval="+INTERVAL+"&limit="+LIMIT+"&agg="+AGG+"&keys="+SOURCE_TB_DEVICE_KEY+"&startTs="+STARTTS+"&endTs="+ENDTS,headers=headers)
        print("Response code: ", r.status_code)
    except Exception as e:
        print("\nAn exception occurred while trying to obtain and print the timeseries from SOURCE Thigsboard: ", e)
        sys.exit(1)

    # Define the timeseries to upload
    TIMESERIES = r.json()
    print("Timseries: ", TIMESERIES)
    
    # Return the result of the GET request
    return TIMESERIES
	
# *** POST section, via MQTT ***
def postData(requestToUpload):
    
    # Save the data to upload in a local variable
    TIMESERIES = requestToUpload
    
    # Create MQTT client
    client = mqtt.Client()

    # Set access token
    client.username_pw_set(TARGET_TB_DEVICE_TOKEN)

    # Connect to ThingsBoard using default MQTT port and 60 seconds keepalive interval
    client.connect(TARGET_TB_ADDRESS, int(TARGET_TB_PORT), 60)
    client.loop_start()

    # Declare data format
    data = {"ts":0, "values":{TARGET_TB_DEVICE_KEY:0}}

    # Upload the timeseries with the proper timestamps and values
    try:
        # Send all the TIMESERIES values via MQTT, START FROM THE LAST ELEMENT SINCE IT IS THE OLDEST ONE
        for i in range(len(TIMESERIES[SOURCE_TB_DEVICE_KEY])-1, -1, -1):

            value = TIMESERIES[SOURCE_TB_DEVICE_KEY][i]['value']
            ts = TIMESERIES[SOURCE_TB_DEVICE_KEY][i]['ts']

            data['ts'] = ts
            data['values'][TARGET_TB_DEVICE_KEY] = value

            # Send data to ThingsBoard via MQTT
            client.publish('v1/devices/me/telemetry', json.dumps(data), 1)
            print("Upload timestamp: ", datetime.fromtimestamp(ts/1000), " | Raw data: ", data)

            # THE DELAY IS NECESSARY TO AVOID THE "WEB_SOCKET: TOO MANY REQUESTS" ERROR
            time.sleep(0.01)

    except KeyboardInterrupt:
        print("\nThe user manually interrputed the MQTT upload using keyboard.")
        pass
    else:
        print("\nData successfully published via MQTT.")

    # Close the MQTT connections
    client.loop_stop()
    client.disconnect()
	
# *** Source Thingsboard variables ***
SOURCE_TB_ADDRESS = 'x.x.x.x'
SOURCE_TB_PORT = '1883'
SOURCE_TB_USER = 'xxxx'
SOURCE_TB_PASSWORD = 'xxxx'
SOURCE_TB_DEVICE_ID = '41eaa800-70c0-11e9-bb10-25bdd88b08cf'
SOURCE_TB_DEVICE_KEY = 'ActivePowerkW'

# *** Target Thingsboard variables ***
TARGET_TB_ADDRESS = 'x.x.x.x'
TARGET_TB_PORT = '1883'
TARGET_TB_DEVICE_TOKEN = 'sOGiLZNUMDYg51Ky75pv'
TARGET_TB_DEVICE_KEY = 'ActivePowerkW'

# *** Timeseries specifications ***
'''
Description of the parameters:
    - startTs: unix timestamp which identifies start of the interval in milliseconds.
    - endTs: unix timestamp which identifies end of the interval in milliseconds.
    - interval (optional): the aggregation interval, in milliseconds.
    - agg (optional): the aggregation function. One of MIN, MAX, AVG, SUM, COUNT, NONE.
    - limit (optional): the max amount of data points to return or intervals to process.
'''
startDate = datetime.strptime('17.05.2019 00:00:00,00','%d.%m.%Y %H:%M:%S,%f')
startDateTs = str(int(startDate.timestamp() * 1000))
STARTTS = startDateTs
endDate = datetime.strptime('18.05.2019 00:00:00,00','%d.%m.%Y %H:%M:%S,%f')
endDateTs = str(int(endDate.timestamp() * 1000))
ENDTS = endDateTs
INTERVAL = '0'
LIMIT = '200000'
AGG = 'NONE'

# *** Main ***
# Define the different keys to upload (defined in SOURCE thingsboard device)
keys = ["ActivePowerkW","EnergyFYkWh","EnergyMonthkWh"]

# Define the different target/source ID/tokens
sourceIDs = ["41eaa800-70c0-11e9-bb10-25bdd88b08cf",
"41faad90-70c0-11e9-bb10-25bdd88b08cf",
"420757c0-70c0-11e9-bb10-25bdd88b08cf"]

targetTOKENs = ["sOGiLZNUMDYg51Ky75pv",
"m0vhaOmjZOlbiS63LuwX",
"wQIRpd0utW2UjqICrhRY"]

# For every device of the list, migrate the data
for k in range(len(sourceIDs)):

    #Update targetTOKEN and sourceID
    SOURCE_TB_DEVICE_ID = sourceIDs[k]
    TARGET_TB_DEVICE_TOKEN = targetTOKENs[k]
	
    print(" ----> MIGRATING ", SOURCE_TB_DEVICE_ID , " TO ", TARGET_TB_DEVICE_TOKEN)
	
    # Iterate and uplaod all the keys of a specifc device
    for i in range(len(keys)):
    
        # Update key to upload
        SOURCE_TB_DEVICE_KEY = keys[i]
        TARGET_TB_DEVICE_KEY = SOURCE_TB_DEVICE_KEY
        print("***** UPLOAD KEY: ", SOURCE_TB_DEVICE_KEY, " *****")

        # Get data from SOURCE thingsboard
        risultatoGetRequest = getData()
        # Send data to TARGET thingsboard
        postData(risultatoGetRequest)
