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

def schedulePings():
    list_of_pairs = [loop.call_soon(pingFace, f1, f2, seq_num) for f1 in valid_faces for f2 in valid_faces if f1 != f2]
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
    updateStatus(interest, 0)

def onTimeout(interest):
    print("Timeout")
    updateStatus(interest, 1)

def onNack(interest, nack):
    print("Nack")
    updateStatus(interest, 2)

def decomposeName(name):
    src = name.getSubName(13, 1)
    dst = name.getSubName(6, 1)
    return src, dst

def updateStatus(interest, status):
    src, dst = decomposeName(interest.getName())
    conn.execute("SELECT FROM testbed_status WHERE src=? AND dst=?", (src, dst))
    if not conn.fetchone():
        conn.execute("INSERT INTO testbed_status (?, ?, ?)", (src, dst, status))
    else:
        conn.execute("UPDATE testbed_status SET status=? WHERE src=? AND dst=?", (status, src, dst))

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
    valid_faces.append(prefix)

def first_run():
    connection = sqlite3.connect("status.db")
    print("connection")
    conn = connection.cursor()
    conn.execute("CREATE TABLE testbed_status (src text, dst text, status integer)")
    conn.execute("INSERT INTO testbed_status VALUES ('worm', 'worm2', 0)")
    conn.close()
    #conn.execute("SELECT * FROM testbed_status")
    #print(conn.fetchone())

#TODO: Test if DB not initialized, else do
first_run()

testbed_json = json.load(open(request.urlretrieve("http://ndndemo.arl.wustl.edu/testbed-nodes.json")[0], "r"))
fch_testbed = [ value for value in testbed_json.values() if value['fch-enabled'] != False ]
#Initialize keychain
keychain = KeyChain()
loop = asyncio.get_event_loop()
for hub in fch_testbed:
    print("Adding faces to hub: {}".format(hub["name"]))
    face_base = hub["site"].strip("http").replace(":80/",":6363")
    print(face_base)
    for protocol in PROTOCOLS:
        face = ThreadsafeFace(loop, "{}{}".format(protocol, hub))
        face.setCommandSigningInfo(keychain, keychain.getDefaultCertificateName())
        prefix = Name(PREFIX + hub["shortname"])
        options = RegistrationOptions().setOrigin(65)
        face.registerPrefix(prefix, onInterest, onRegisterFailed, onRegisterSuccess=registration, registrationOptions=options)

print("Begin ping")
loop.call_later(10, schedulePings)
time.sleep(30)
while loop.is_running():
    pass
