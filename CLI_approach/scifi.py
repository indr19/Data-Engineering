#!/usr/bin/env python
import json
from kafka import KafkaProducer
from flask import Flask, request

app = Flask(__name__)
producer = KafkaProducer(bootstrap_servers='kafka:29092')

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

@app.route("/")
def default_response():
    default_event = {'event_type': 'default'}
    log_to_kafka('starfighter', default_event)
    return "This is the default response!\n"

@app.route("/Enrol_pilot")
def enrol_pilot():
    starfighter_data = request.get_json()
    pilotID    = starfighter_data['pilotID']
    pilot      = starfighter_data['pilot']
    ship       = starfighter_data['ship']
    allegiance = starfighter_data['allegiance']
    ship_cost  = starfighter_data['ship_cost']
    datestamp  = starfighter_data['datestamp']
    enrol_pilot_event = {'event_type': 'USER_data',
                         'pilotID' : pilotID, 
                         'pilot' : pilot, 
                         'ship' : ship,
                         'allegiance' : allegiance, 
                         'ship_cost' : ship_cost, 
                         'datestamp' : datestamp }
    log_to_kafka('starfighter', enrol_pilot_event)
    return ("%s you are successfully enrolled pilot in the %s. Now go clear the starways of your enemies!\n" % (pilot , allegiance))

@app.route("/Fit_ship")
def fit_ship():
    fitting_data = request.get_json()
    pilotID    = fitting_data['pilotID']
    pilot      = fitting_data['pilot']
    missile_launchers = fitting_data['missile_launchers']
    cannons    = fitting_data['cannons']
    missiles   = fitting_data['missiles']
    torpedoes  = fitting_data['torpedoes']
    fit_ship_event = {'event_type': 'USER_fit',
                      'pilotID' : pilotID, 
                      'pilot' : pilot, 
                      'missile_launchers' : missile_launchers, 
                      'cannons' : cannons,
                      'missiles' : missiles,
                      'torpedoes': torpedoes } 
    log_to_kafka('starfighter', fit_ship_event)
    return ("%s you have successfully reconfigured your ship with upgraded weapons.  Slip dry-dock and fight your ship!\n"%pilot)
