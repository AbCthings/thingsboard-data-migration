'''
@Author Alessandro Bortoletto - LINKS Foundation

@Version 0.0.2

@Date 26-06-2019

@Brief This is a collection of useful functions to perform operations with ThingsBoard DB via RESTful API or MQTT

'''

# *** Import section ***
import requests
from datetime import datetime
import time
import sys
import paho.mqtt.client as mqtt
import json

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
    
    INTERVAL = '0'
    LIMIT = '200000'
    AGG = 'NONE'
    
    # Define some constants
    
    # Define the headers of the authorization request
    headersToken = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    }

    # Define the body of the authorization request
    data = '{"username":"'+user+'", "password":"'+password+'"}'

    # Perform the POST request to obtain X-Token Authorization
    try:
        response = requests.post('http://'+ip+':'+port+'/api/auth/login', headers=headersToken, data=data)
        X_AUTH_TOKEN = response.json()['token']
        print("Codice autorizzazione: ",X_AUTH_TOKEN)
    except Exception as e:
        print("\nAn exception occurred while trying to obtain the authorization from SOURCE Thigsboard: ", e)
        sys.exit(1)

    # Define the headers of the request
    headers = {'Accept':'application/json','X-Authorization': 'Bearer '+X_AUTH_TOKEN}

    # Perform the GET request to obtain timeseries
    try:
        r = requests.get("http://"+ip+":"+port+"/api/plugins/telemetry/DEVICE/"+deviceId+"/values/timeseries?interval="+INTERVAL+"&limit="+LIMIT+"&agg="+AGG+"&keys="+key+"&startTs="+startTs+"&endTs="+endTs,headers=headers)
        print("Response code: ", r.status_code)
    except Exception as e:
        print("\nAn exception occurred while trying to obtain and print the timeseries from SOURCE Thigsboard: ", e)
        sys.exit(1)

    # Define the timeseries to upload
    TIMESERIES = r.json()
    print("Timseries: ", TIMESERIES)
    
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
        for i in range(len(timeseries[key])-1, -1, -1):

            value = timeseries[key][i]['value']
            ts = timeseries[key][i]['ts']

            data['ts'] = ts
            data['values'][key] = value

            # Send data to ThingsBoard via MQTT
            client.publish('v1/devices/me/telemetry', json.dumps(data), 1)
            print("Upload timestamp: ", datetime.fromtimestamp(ts/1000), " | Raw data: ", data)

            # THE DELAY IS NECESSARY TO AVOID THE ThingsBoard "WEB_SOCKET: TOO MANY REQUESTS" ERROR
            time.sleep(0.01)

    except KeyboardInterrupt:
        print("\nThe user manually interrputed the MQTT upload using keyboard.")
        pass
    else:
        print("\nData successfully published via MQTT.")

    # Close the MQTT connections
    client.loop_stop()
    client.disconnect()
