#! /usr/bin/python
import sys,os
import re
import csv
import datetime
from pymongo import MongoClient

client = MongoClient()
db = client.kddproject_v2

traj = []
counter = 0
airsigmentarea_path = '/Volumes/Seagate Backup Plus Drive/KDD/Data/flightstats_airsigmetarea.csv'
with open(airsigmentarea_path, 'rb') as asfile:
	rows = csv.reader(asfile, delimiter=',')
	next(rows, None)
	for row in rows:
		ordinal = int(row[3])
		if (ordinal == 0):
			if traj:
				
				result = db.airsigmets.update_one(
    					{"airsigmetid": airsigmetid},
    					{
        					"$set": {
            						"sigmetarea": traj
        					},
    					}
				)
				
			traj = []
			airsigmetid = int(row[0])
			loc = ()
		else:
			latitude = float(row[1])
			longitude = float(row[2])
			loc = (latitude,longitude)
			traj.append(loc)
