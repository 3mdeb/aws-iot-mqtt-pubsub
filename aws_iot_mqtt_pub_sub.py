#!/usr/bin/python3.4

import logging
import logging.handlers
import sys
import os
import ssl
import paho.mqtt.client as mqtt
import threading
import time
import queue
import urllib.request
import socket
import subprocess
import json

LOG_NAME = 'aws-iot-mqtt-pubsub'
LOG_LEVEL = logging.DEBUG
LOG_SIZE = 10*1024*1024
LOG_BACKUPS_COUNT = 5
ETH0_ADDRESS_PATH = "/sys/class/net/eth0/address"

# netvision endpoint
AWS_MQTT_HOST = "A1SW3Q5MUSFA0L.iot.eu-west-1.amazonaws.com"

AWS_MQTT_PORT = 8883
AWS_MQTT_SHADOW_TOPIC_PREFIX = '$aws/things/'
CA_CERT = "/home/pi/certs/aws-iot-rootCA.crt"
AWS_PEM = "/home/pi/certs/pem"
AWS_KEY = "/home/pi/certs/priv_key"

MESSAGE_KEYS = ["host", "path", "key"]
REQUEST_QUEUE_SIZE = 10
MAX_REQUEST_WAIT_TIME = 10
HASH_FLAG = 0


def setup_logger():
    logger = logging.getLogger()

    formatter = logging.Formatter("%(asctime)s [%(levelname)-8s]"
                                  "%(funcName)s:%(lineno)s "
                                  "%(message)s")

    file_handler = logging.handlers.RotatingFileHandler(
        "{0}.log".format(LOG_NAME), maxBytes=LOG_SIZE,
        backupCount=LOG_BACKUPS_COUNT)
    file_handler.setFormatter(formatter)

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(LOG_LEVEL)


def on_message(client, userdata, msg):
    log.info("incoming message (" + msg.topic + ")")
    if msg.topic == AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
            "/shadow/update/delta":
        log.info("processing delta message")
        log.info("payload:{0}\n userdata:{1}".format(msg.payload.decode(), userdata))
        j = json.loads(msg.payload.decode())
        if "led" in j.keys():
            log.info("set LED state to: {0}".format(j['led']))
            set_led_state(j['led'])
        else:
            log.info("other command")
    elif msg.topic == AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
            "/shadow/update/accepted":
        log.info("message state accepted")
    elif msg.topic == AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
            "/shadow/update/rejected":
        log.error("message state rejected. Reason: {0}"
                  .format(str(msg.payload)))


def on_connect(client, userdata, rc):
    log.info("connection returned result: " + str(rc))
    topic_delta = AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
        "/shadow/update/delta"
    log.info("subscribing: {0}".format(topic_delta))
    topic_update_accepted = AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
        "/shadow/update/accepted"
    log.info("subscribing: {0}".format(topic_update_accepted))
    topic_update_rejected = AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
        "/shadow/update/rejected"
    log.info("subscribing: {0}".format(topic_update_rejected))
    client.subscribe([(topic_delta, 1),
                      (topic_update_accepted, 1),
                      (topic_update_rejected, 1)])


def on_disconnect(client, userdata, rc):
    log.info("disconnect result: " + str(rc))


def on_subscribe(client, userdata, mid, granted_qos):
    log.info("subscribed. MID = " + str(mid) + ". Granted QoS = " +
             str(granted_qos))


def on_unsubscribe(client, userdata, rc):
    log.info("unsubscribe result: " + str(rc))


def on_log(client, userdata, level, buf):
    log.info("buf: {0} - {1}".format(str(level), buf))


def on_publish(client, obj, flags):
    log.info('message published')

def set_led_state(state):
    import RPi.GPIO as GPIO
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(7, GPIO.OUT)
    GPIO.output(7, state)

def get_id():
    try:
        if os.path.exists(ETH0_ADDRESS_PATH):
            with open(ETH0_ADDRESS_PATH) as f:
                address = f.read().replace(":", "")
        else:
            logger = logging.getLogger()
            logger.warning("{0}: file does not exists"
                           .format(ETH0_ADDRESS_PATH))
            return "unknown"
    except Exception as e:
        log.info(e)
        return "unknown"
    else:
        return address.strip()

if __name__ == "__main__":

    this_id = get_id()
    setup_logger()
    log = logging.getLogger()
    log.info(" aws-iot-mqtt-pubsub@" + this_id)

    while True:
        try:
            urllib.request.urlopen('http://www.google.com', timeout=2)
            mqttc = mqtt.Client(client_id=this_id, userdata=this_id)
            mqttc.on_log = on_log
            mqttc.on_message = on_message
            mqttc.on_connect = on_connect
            mqttc.on_disconnect = on_disconnect
            mqttc.on_subscribe = on_subscribe
            mqttc.on_unsubscribe = on_unsubscribe
            mqttc.on_publish = on_publish
            while not (os.path.isfile(CA_CERT)
                       and os.path.isfile(AWS_PEM)
                       and os.path.isfile(AWS_KEY)):
                log.error('lack of cert or keys, waiting ... ')
                time.sleep(1)
            mqttc.tls_set(CA_CERT, certfile=AWS_PEM, keyfile=AWS_KEY,
                          tls_version=ssl.PROTOCOL_TLSv1_2)
            log.info("aws-iot-mqtt-pubsub connecting: {0}:{1}"
                     .format(AWS_MQTT_HOST, AWS_MQTT_PORT))
            mqttc.connect(AWS_MQTT_HOST, AWS_MQTT_PORT, 60)
            break
        except urllib.request.URLError:
            log.debug("no internet connection wait {0}sec"
                      .format(GET_IP_DELAY))
            time.sleep(GET_IP_DELAY)
        except socket.timeout:
            log.debug("time out, retry, wait {0}sec".format(GET_IP_DELAY))
            time.sleep(GET_IP_DELAY)
        except:
            log.exception("unable to initialize MQTT communication")
            sys.exit()

    try:
        mqttc.loop_forever()
    except KeyboardInterrupt:
        pass
    try:
        log.info("ending...")
    except:
        log.exception("unable to stop shadow-daemon")
        sys.exit()
