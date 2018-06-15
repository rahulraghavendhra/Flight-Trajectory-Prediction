#! /usr/bin/python
import sys,os
import re
import csv
import datetime
from pymongo import MongoClient

## Function to get trajectories from db for all typical trajectory ids
def get_typical_trajectories(typical_trajectories_id_path,db):

## Getting typical trajectory ids from csv file
	typ_ids=[]
	with open(typical_trajectories_id_path, 'r') as idfile:
		#data = idfile.read().strip().replace('\n','')
		counter = 0
		for row in idfile:
			counter = counter + 1
			row = row.rstrip('\n')
			typ_ids.append(int(row))
	
	print typ_ids
## Query to find typical trajectories from typical trajecory ids
	cur2 = db.histflightplan_v2.find(
		{
      			"flightplanid": { "$in": typ_ids }
		},
		{
			"_id":0,"flightplanid":1,"originalarrivalutc":1,"originaldepartureutc":1,"lat":1,"long":1,"orderedordinal":1
		}
	).sort([("flightplanid",1),("orderedordinal",1)])
	print(cur2.count())
## Write to csv file after converting to list
	typical_trajectories_path = '/Volumes/Seagate Backup Plus Drive/KDD/Working_Directory/typical_trajectories.csv'
	counter = 0
	with open(typical_trajectories_path, 'w') as trajfile:
		fields = ['flightplanid', 'originalarrivalutc','originaldepartureutc','lat', 'long','orderedordinal']
		writer = csv.DictWriter(trajfile, fieldnames=fields)
		writer.writeheader()
		results = list(cur2[:])
		for entries in results:
			print(counter)
			#print(entries)      	
			writer.writerow(entries)
	    		counter += 1
			
if __name__=='__main__':
	client = MongoClient('localhost',27017,maxPoolSize=None)
	db = client.kddproject_v2
	typical_trajectories_id_path = '/Volumes/Seagate Backup Plus Drive/KDD/Working_Directory/typicaltrajectory-flightid.csv'
	get_typical_trajectories(typical_trajectories_id_path,db)

