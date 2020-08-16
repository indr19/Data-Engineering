#!/usr/bin/env python
import os
from flask import Flask, render_template, request, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
import datetime
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers='kafka:29092')
print(dir(datetime))
transaction_time = datetime.datetime.utcnow()

project_dir = os.path.dirname(os.path.abspath(__file__))
database_file = "sqlite:///{}".format(os.path.join(project_dir, "pilotData.db"))

app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = database_file

db = SQLAlchemy(app)

class Data(db.Model):
    pilotID = db.Column(db.Integer, primary_key=True)
    pilot = db.Column(db.String(100))
    allegiance = db.Column(db.String(100))
    ship = db.Column(db.String(100))
    ship_cost = db.Column(db.String(100))
    time = db.Column(db.String(100))

    def __init__(self, pilot, allegiance,ship,ship_cost,time):
        self.pilot = pilot
        self.allegiance = allegiance
        self.ship = ship
        self.ship_cost = ship_cost
        self.time = time

def log_to_kafka(topic, event):
    event.update(request.headers)
    producer.send(topic, json.dumps(event).encode())

# this route is for inserting data to mysql database via html forms
@app.route('/insert', methods=['POST'])
def insert():
    if request.method == 'POST':
        pilot = request.form['pilot']
        allegiance = request.form['allegiance']
        ship = request.form['ship']
        ship_cost = request.form['ship_cost']
        time = transaction_time
        my_data = Data(pilot, allegiance, ship,ship_cost,time)
        db.session.add(my_data)
        db.session.commit()
        id= len(Data.query.all())
        print("id :: ",id)

        enrol_pilot_event = {
            'event_type': 'USER_data',
            "pilotID": id,
            "pilot": pilot,
            "allegiance": allegiance,
            "ship": ship,
            "ship_cost": ship_cost,
            "datestamp": time
        }
        log_to_kafka('starfighter', enrol_pilot_event)
        return redirect(url_for('home'))

@app.route("/")
def home():
    pilots = Data.query.all()
    print("len(pilots)::",len(pilots))
    #Log to Kafka
    default_event = {'event_type': 'default'}
    log_to_kafka('starfighter', default_event)

    return render_template("index.html",pilots=pilots)

if __name__ == "__main__":
    app.run(debug=True)
