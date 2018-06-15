#! /usr/bin/python
import sys,os
import re
import csv
import datetime
from pymongo import MongoClient

## Function to get all flight trajectories from db for departure and arrival
def get_all_flight_trajectories_from_db(departure,arrival,db):

## Query to get from mongodb and store output in results
	db.histflightplan_v2.aggregate(
		[
			{"$match": {"departureairport": departure, "arrivalairport": arrival} },
	   		{"$group": {"_id":"$flightplanid","lat": {"$avg": "$lat" }, "long": {"$avg": "$long" } } },
			{"$out": "results"}
		] 
	)

## All_Flight_Trajectories_Path
	all_flight_trajectories_path = '/Volumes/Seagate Backup Plus Drive/KDD/Working_Directory/data.csv'

## Exporing to csv using mongoexport
	cmd = 'mongoexport -d kddproject_v2 -c results -f _id,lat,long --type=csv | tail -n+2 > "' + all_flight_trajectories_path + '"'
	os.system(cmd)

if __name__=='__main__':
	client = MongoClient('localhost',27017,maxPoolSize=None)
	db = client.kddproject_v2
	get_all_flight_trajectories_from_db("DFW","ORD",db)
	
