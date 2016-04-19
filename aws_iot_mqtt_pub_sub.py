#!/usr/bin/python3.4

import logging
import logging.handlers
import sys
import os
import ssl
import threading
import time
import queue
import urllib.request
import socket
import subprocess
import json
import RPi.GPIO as GPIO
import paho.mqtt.client as mqtt

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

ALARM_PIN = 12
LOCK_PIN = 7
RESET_PIN = 13
WINDOW_PIN = 11

reset_queue = queue.Queue(1)

class StateReporter(threading.Thread):

    def __init__(self, mac, log, mqttc):
        super().__init__()
        self.log = log
        self.mqttc = mqttc
        self.mac = mac
        self.start()

    def __publish(self, json):
        self.log.info("json:{0}".format(json))
        topic = "$aws/things/{0}/shadow/update".format(self.mac)
        self.mqttc.publish(topic, payload=json)

    def run(self):
        try:
            while True:
                j = {}
                j["state"] = {}
                j["state"]["reported"] = {}
                j["state"]["reported"]["lock"] = GPIO.input(LOCK_PIN)
                j["state"]["reported"]["alarm"] = GPIO.input(ALARM_PIN)
                j["state"]["reported"]["reset"] = GPIO.input(RESET_PIN)
                j["state"]["reported"]["window"] = GPIO.input(WINDOW_PIN)
                self.__publish(json.dumps(j))
                time.sleep(5)
        except:
            self.log.exception("unable to report state")
            return

    def end(self):
        self.log.info("stopping reporter thread")
        self.running = False

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
    # log.info("incoming message (" + msg.topic + ")")
    if msg.topic == AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
            "/shadow/update/delta":
        log.info("processing delta message")
        log.info("payload:{0}\n userdata:{1}".format(msg.payload.decode(), userdata))
        j = json.loads(msg.payload.decode())
        if "lock" in j["state"].keys():
            log.info("set LED state to: {0}".format(j["state"]["lock"]))
            GPIO.output(LOCK_PIN, j["state"]["lock"])
        if "reset" in j["state"].keys():
            if j["state"]["reset"] and reset_queue.qsize() == 0:
                log.debug("reset alarm")
                reset_queue.put(False)

    elif msg.topic == AWS_MQTT_SHADOW_TOPIC_PREFIX + userdata + \
            "/shadow/update/accepted":
        # log.info("message state accepted")
        pass
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
    # log.debug("buf: {0} - {1}".format(str(level), buf))
    pass


def on_publish(client, obj, flags):
    # log.debug('message published')
    pass

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

def alarm(channel):
    log.info("channel {0}".format(channel))
    reset_state = True
    time.sleep(0.25)
    if GPIO.input(WINDOW_PIN):
       return
    while (reset_state):
        GPIO.output(ALARM_PIN, True)
        time.sleep(0.5)
        GPIO.output(ALARM_PIN, False)
        time.sleep(0.5)
        try:
            log.info("queue before {0}".format(reset_queue.qsize()))
            reset_state = reset_queue.get_nowait()
            log.info("queue after {0}".format(reset_queue.qsize()))
            log.info("reset_state: {0}".format(reset_state))
        except queue.Empty:
            log.debug("queue empty")


if __name__ == "__main__":

    this_id = get_id()
    setup_logger()
    log = logging.getLogger()
    log.info(" aws-iot-mqtt-pubsub@" + this_id)

    log.info("setup GPIO")
    GPIO.setmode(GPIO.BOARD)
    GPIO.setup(LOCK_PIN, GPIO.OUT)
    GPIO.setup(WINDOW_PIN, GPIO.IN, pull_up_down=GPIO.PUD_DOWN)
    GPIO.setup(ALARM_PIN, GPIO.OUT)
    GPIO.setup(RESET_PIN, GPIO.OUT)
    GPIO.setup(16, GPIO.OUT)
    GPIO.output(16, True)
    GPIO.add_event_detect(11, GPIO.FALLING, callback=alarm)

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
        reporter = StateReporter(this_id, log, mqttc)
    except:
        log.exception("unable to run state reporter")

    try:
        mqttc.loop_forever()
    except KeyboardInterrupt:
        GPIO.output(LOCK_PIN, False)
        GPIO.output(ALARM_PIN, False)
        GPIO.output(RESET_PIN, False)

    try:
        log.info("ending...")
        reporter.end()
    except:
        log.exception("unable to stop shadow-daemon")
        sys.exit()
