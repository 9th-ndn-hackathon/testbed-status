from pyndn import Name, Interest, Data
from pyndn.registration_options import RegistrationOptions
from pyndn.security import KeyChain
from pyndn.threadsafe_face import ThreadsafeFace
from pyndn.util import Blob
from urllib import request
import asyncio
import json
import os
import sqlite3
import sys
import time

PROTOCOLS = ("udp", "tcp", "wss")
PREFIX = "/ndn/edu/ucla/%40GUEST/davidepesa%40gmail.com/pinger/"
valid_faces = []
seq_num = 0
conn = None

def schedulePings(prefix):
    f1 = prefix
    list_of_pairs = [loop.call_soon(pingFace, f1, f2, seq_num) for f2 in valid_faces if f1 != f2]
    seq_num += 1
    #loop.call_later(30, schedulePings)

def pingFace(srcFace, dstPrefix, iterNumber):
    print("Will ping from", srcFace, dstPrefix, "iterNumber:", iterNumber)

    face = valid_faces[srcFace]
    name = Name(dstPrefix)
    #name.append("ping")
    name.append(Name(srcFace))
    name.appendSequenceNumber(iterNumber)

    #print("name:", name)
    interest = Interest(name)

    face.expressInterest(interest, onData, onTimeout, onNack)
    face.processEvents()

def onData(interest, data):
    print("Data received")
    updateStatus(interest, 'data')

def onTimeout(interest):
    print("Timeout received")
    updateStatus(interest, 'interest')

def onNack(interest, nack):
    print("Nack received")
    updateStatus(interest, 'nack')

def decomposeName(name):
    src = name.getSubName(13, 1)
    dst = name.getSubName(6, 1)
    return src, dst

def updateStatus(interest, receieved):
    src, dst = decomposeName(interest.getName())
    conn.execute("SELECT FROM protocol_status WHERE timestamp=? AND src=? AND dst=?", (timestamp, src, dst))
    
    if not conn.fetchone():
        conn.execute("INSERT INTO protocol_status (?, ?, ?, 0, 0, 0)", (timestamp, src, dst))
    conn.execute("UPDATE protocol_status SET ? = ? + 1 WHERE timestamp=? AND src=? AND dst=?", (received, receieved, timestamp, src, dst))

def onInterest(prefix, interest, face, interestFilterId, filter):
    data = interest.getName()
    print("Incoming interest: {}".format(data))
    data.setContent(Blob("TEST"))
    keychain.sign(data, keychain.getDefaultCertificateName())
    try:
        face.putData(data)
    except Exception as exception:
        print("Error in transport.send: %s", str(exception))

def onRegisterFailed(prefix):
    print("Registration failed for prefix {}".format(prefix.toUri()))

def registration(prefix, prefixID):
    print("Registration succeeded for prefix {}".format(prefix.toUri()))
    #valid_faces.append(prefix)

def first_run():
    connection = sqlite3.connect("status.db")
    print("connection")
    conn = connection.cursor()
    conn.execute("CREATE TABLE propagation_status (timestamp integer, src text, dst text, data integer, nack integer, timeout integer)")
    conn.execute("CREATE TABLE protocol_status (timestamp integer, node text, protocol text, data integer, nack integer, timeout integer)")
    conn.close()

#TODO: Test if DB not initialized, else do
#first_run()
timestamp = int(time.time())
#sys.exit(1)
testbed_json = json.load(open(request.urlretrieve("http://ndndemo.arl.wustl.edu/testbed-nodes.json")[0], "r"))
fch_testbed = [ value for value in testbed_json.values() if value['fch-enabled'] != False ]
#Initialize keychain
keychain = KeyChain()
loop = asyncio.get_event_loop()
for hub in fch_testbed:
    valid_prefixes = Name(PREFIX + hub["shortname"])

for hub in fch_testbed:
    print("Adding faces to hub: {}".format(hub["name"]))
    face_base = hub["site"].strip("http").replace(":80/",":6363")
    for protocol in PROTOCOLS:
        face = ThreadsafeFace(loop, "{}{}".format(protocol, hub))
        face.setCommandSigningInfo(keychain, keychain.getDefaultCertificateName())
        prefix = Name(PREFIX + hub["shortname"])
        options = RegistrationOptions().setOrigin(65)
        face.registerPrefix(prefix, onInterest, onRegisterFailed, onRegisterSuccess=registration, registrationOptions=options)
<<<<<<< HEAD
        #time.sleep(2)
        print("Begin ping")
        loop.call_soon(schedulePings, prefix)
        while loop.is_running():
            pass
time.sleep(30)
=======

print("Begin ping")
loop.call_later(10, schedulePings)
time.sleep(30)
while loop.is_running():
    pass
>>>>>>> 6ea443584340d0bc3cb68b46a24584b68ad0dc17
