# How to run me 🏃

## Run with the jar 

``java -jar mqtt-broker-api-0.0.1-SNAPSHOT.jar.jar``

## Run with docker`

First build the flat jar with the usual.
``gradle bootJar``

Then run the docker build
``docker build -t mqttbroker:latest .``

And then run 
``docker run mqttbroker:latest``


## What it serves for

With the mqtt broker app you can maintain different broker configurations.

Connect to them.

Publish String messages (this is a know limitation in this app for now and a future work).

Subscribe long polling from diffrent brokers.
