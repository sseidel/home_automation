#!/usr/bin/python3
# -*- coding: utf-8 -*-
# stolen from https://github.com/turbosnute/tibberpulse-influxdb/blob/master/pulse.py
import sys
import websocket
import traceback
import ssl
import json
import _thread
import time
import datetime
import calendar
import os
import logging
#import asyncio

from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport

from dateutil.parser import parse
import requests

def str_to_bool(v: str) -> bool:
    # Interpret string as bool
    return v.lower() in ("yes", "true", "t", "1")

print("tibberpulse-influxdb")


global adr
adr = "DEFAULT"
tibbertoken=""
tibberhomeid=""
headers = {"Authorization": "Bearer "+tibbertoken}

subscription_query = ""


def ifStringZero(val):
    val = str(val).strip()
    if val.replace('.','',1).isdigit():
      res = float(val)
    else:
      res = None
    return res

def console_handler(data):
    if 'liveMeasurement' in data:
        
        measurement = data['liveMeasurement']
        timestamp = measurement['timestamp']
        print(measurement)
        timeObj = parse(timestamp)
        hourMultiplier = timeObj.hour+1
        daysInMonth = calendar.monthrange(timeObj.year, timeObj.month)[1]
        power = measurement['power']
        #min_power = measurement['minPower']
        #max_power = measurement['maxPower']
        #avg_power = measurement['averagePower']
        accumulated = measurement['accumulatedConsumption']
        accumulated_cost = measurement['accumulatedCost']
        #currency = measurement['currency']
        voltagePhase1 = measurement['voltagePhase1']
        voltagePhase2 = measurement['voltagePhase2']
        voltagePhase3 = measurement['voltagePhase3']
        currentL1 = measurement['currentL1']
        currentL2 = measurement['currentL2']
        currentL3 = measurement['currentL3']
        lastMeterConsumption = measurement['lastMeterConsumption']
        output = [
        {
            "measurement": "pulse",
            "time": timestamp,
            "tags": {
                "address": adr
            },
            "fields": {
                "power": ifStringZero(power),
                "consumption": ifStringZero(accumulated),
                "cost": ifStringZero(accumulated_cost),
                "voltagePhase1": ifStringZero(voltagePhase1),
                "voltagePhase2": ifStringZero(voltagePhase2),
                "voltagePhase3": ifStringZero(voltagePhase3),
                "currentL1": ifStringZero(currentL1),
                "currentL2": ifStringZero(currentL2),
                "currentL3": ifStringZero(currentL3),
                "lastMeterConsumption": ifStringZero(lastMeterConsumption),
                "hourmultiplier": hourMultiplier,
                "daysInMonth": daysInMonth
            }
        }
        ]
        output_csv = f"{timestamp};{ifStringZero(power)};{ifStringZero(accumulated)};{ifStringZero(lastMeterConsumption)}\n"
        #print(output)
        print(output_csv)
        with open("tibber_pulse_data.csv", "a") as file:
            file.writelines(output_csv)

        
    else:
        print(data)

def run_query(query, headers): # A simple function to use requests.post to make the API call. Note thejson= section.
    request = requests.post('https://api.tibber.com/v1-beta/gql', json={'query': query}, headers=headers)
    if request.status_code == 200:
        return request.json()
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))

def fetch_data(url, subscription_query, headers):
    transport = WebsocketsTransport(
        url=url,
        headers=headers,
        keep_alive_timeout=120
    )

    ws_client = Client(
        transport=transport,
        fetch_schema_from_transport=True
    )

    subscription = gql(subscription_query)

    try:
       for result in ws_client.subscribe(subscription):
          console_handler(result)
    except Exception as ex:
       traceback.print_exc()
       module = ex.__class__.__module__
       print(module + ex.__class__.__name__)
       exargs = str(ex.args)
       if exargs.find("Too many open connections") != -1:
          print("Too many open connections. Sleeping 10 minutes...")
          print("If you continue to see this error you can fix it by recreating the tibber token")
          time.sleep(600)
if tibbertoken == 'NOTOKEN':
    print("Tibber token is missing!")
else:
    if tibberhomeid == 'NOID':
        #Try to automaticly get homeid:
        query = "{ viewer { homes { address { address1 } id } } }"
        resp = run_query(query, headers)
        id = resp['data']['viewer']['homes'][0]['id']
        tibberhomeid = id
        adr = resp['data']['viewer']['homes'][0]['address']['address1']
        print("Using homeid '"+id+"' ("+adr+")")

    subscription_query = """
    subscription {{
        liveMeasurement(homeId:"{home_id}"){{
            timestamp
            power
            accumulatedConsumption
            accumulatedCost
            voltagePhase1
            voltagePhase2
            voltagePhase3
            currentL1
            currentL2
            currentL3
            lastMeterConsumption
        }}
    }}
    """.format(home_id=tibberhomeid)
    # Get subscription URI
    request_res = run_query("{viewer{websocketSubscriptionUrl}}", headers)
    ws_uri = request_res['data']['viewer']['websocketSubscriptionUrl']

    print("Sleep for 5 secs.")
    time.sleep(5)
    print("Run GQL query.")
