import csv
import numpy as np
import pandas as pd
import time
from time import mktime
from datetime import datetime
from pymongo import MongoClient


def resample_time(starttime, endtime, n):
 
    index = pd.date_range(starttime, endtime, freq='T')
    df = pd.DataFrame(index=index)
    first = df.index.min()
    last = df.index.max()
    secs = int((last-first).total_seconds()//n)
    periodsize = '{:d}S'.format(secs)
    result = df.resample(periodsize, how='sum')
    final_list = []
    for i in range (0,len(result.index)):
        final_list.append(str(result.index[i]))
    return final_list

def resample_trajectory(data, final_length):
    new_data = []
    current_length = len(data)
    for j_dash in range(1, final_length+1):
        j = round((j_dash*current_length)/final_length) 
        new_data.append(data[int(j)-1]);
    return new_data

def getairsigmetareas(time):
	client = MongoClient()
	db = client.kddproject_v2

	dt = datetime.strptime(time,"%Y-%m-%d %H:%M:%S")

	airsigmet_cursor = db.airsigmets.find(
		{
	        	"$and": [{
					"timevalidfromutc": {"$lt" : dt}
	            		},
	           		{
	                		"timevalidtoutc": {"$gte" : dt}
	            		}]
	    	},
	    	{
	        	"airsigmetid":1,"timevalidfromutc":1,"timevalidtoutc":1,"movementdirdegrees":1,"movementspeedknots":1,"hazardtype":1,"hazardseverity":1,"sigmetarea":1
	    	}
	)
	#print(airsigmet_cursor.count())
	airsigmets_res = list(airsigmet_cursor)
	
	airsigmet = {}
	for entries in airsigmets_res:
		key = entries['airsigmetid']
		if key not in airsigmet.keys():
			airsigmet[key] = {}
			airsigmet[key]['validfrom'] = ''
			airsigmet[key]['validto'] = ''
			airsigmet[key]['movementdirdegrees'] = ''
			airsigmet[key]['movementspeedknots'] = ''
			airsigmet[key]['hazardtype'] = ''
			airsigmet[key]['hazardseverity'] = ''
			airsigmet[key]['area'] = []
		#print entries['timevalidfromutc']	
		airsigmet[key]['validfrom'] = entries['timevalidfromutc']
		airsigmet[key]['validto'] = entries['timevalidtoutc']
		airsigmet[key]['movementdirdegrees'] = entries['movementdirdegrees']
		airsigmet[key]['movementspeedknots'] = entries['movementspeedknots']
		airsigmet[key]['hazardtype'] = entries['hazardtype']
		airsigmet[key]['hazardseverity'] = entries['hazardseverity']
		airsigmet[key]['area'] = entries['sigmetarea']
	#This function takes the time of every trajectory points as input, queries the db and then retrieves the airsigment id. The airsigmet id is then queried to get the end points of the polygon.
	#a dictioanry whose key is the airsigmet id and value is the list of tuples e.g., [(0,0), (2,0), (2,2), (0,2)]
#	airsigmet = {}
	airsigmet[3274] = {}
	airsigmet[3274]['area'] = [(32, -117), (35, -117), (32, -119), (35, -119)]
	airsigmet[3274]['validfrom'] = '2012-12-11 00:55:00'
	airsigmet[3274]['validto'] = '2012-12-12 02:55:00'

	return airsigmet

def is_point_in_polygon(x,y,poly):
    n = len(poly)
#    print n;
    inside = False
    p1x,p1y = poly[0]

    for i in range(n+1):
        p2x,p2y = poly[i % n]
#        print "p2x, p2y: ", p2x, p2y
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xints = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x,p1y = p2x,p2y

    return inside

def check_typical_trajectory(datasets, sigmet, index):
    modifiedtuple = []
    for key in datasets:
        dataset_time = datetime.strptime(datasets[key]['time'][index], "%Y-%m-%d %H:%M:%S").time()
        validfrom_time = sigmet['validfrom'].time()
        validto_time = sigmet['validto'].time()
     
        if(dataset_time > validfrom_time and dataset_time < validto_time):
            within_polygon = is_point_in_polygon(datasets[key]['trajectory'][i][0], datasets[key]['trajectory'][i][1], sigmet['area'])
            if within_polygon:
                modifiedtuple.append(datasets[key]['trajectory'][i])
    return modifiedtuple


dataPath = '/Volumes/Seagate Backup Plus Drive/KDD/Working_Directory/typical_trajectories.csv'
inputPath = '/Volumes/Seagate Backup Plus Drive/KDD/Working_Directory/input.csv'

data = []
data_dict = {}
input_dict = {}
with open(dataPath, 'rb') as f:
    reader = csv.reader(f)
    next(reader, None) 
    for point in reader:
        traj_points = []
        if point[0] not in data_dict.keys():
            data_dict[point[0]] = {}
            data_dict[point[0]]['traj'] = []
            data_dict[point[0]]['starttime'] = ""
            data_dict[point[0]]['endtime'] = ""
        data_dict[point[0]]['starttime'] = point[2]
        data_dict[point[0]]['endtime'] = point[1]
        traj_points.append(float(point[3]))
        traj_points.append(float(point[4]))
        data_dict[point[0]]['traj'].append(tuple(traj_points))

with open(inputPath, 'rb') as f1:
    reader = csv.reader(f1)
    input_dict['traj'] = []
    input_dict['starttime'] = '2012-12-09 12:20:00'
    input_dict['endtime'] = '2012-12-09 14:35:00'
    for point in reader:
        traj_points = []
        traj_points.append(float(point[0]))
        traj_points.append(float(point[1]))
        input_dict['traj'].append(tuple(traj_points))

#Resampling the input
input_dict['time'] = []
temp_list = []
input_dict['sampled_trajectory'] = []
input_dict['sampled_trajectory'] = resample_trajectory(input_dict['traj'], 30)
sample_length = len(input_dict['sampled_trajectory'])
input_dict['time'] = resample_time(input_dict['starttime'], input_dict['endtime'], sample_length)
#temp_list = resample_time(input_dict['starttime'], input_dict['endtime'], sample_length)


#Resampling the data
resampled_datasets = {}
for key in data_dict:
    new_traj_data = []
    new_sample_time = []
    if key not in resampled_datasets.keys():
        resampled_datasets[key] = {}
        resampled_datasets[key]['trajectory'] = []
        resampled_datasets[key]['time'] = []
    new_traj_data = resample_trajectory(data_dict[key]['traj'], sample_length)
    new_sample_time = resample_time(data_dict[key]['starttime'], data_dict[key]['endtime'], sample_length)
    resampled_datasets[key]['trajectory'] = new_traj_data
    resampled_datasets[key]['time'] = new_sample_time
modified_trajectory = []
speed = []
dirdegrees = [] 
hazardseverity = []
hazardtype = []
#Checking if the sampled input data lies on the polygon.
for i in range(0, len(input_dict['sampled_trajectory'])):
    modified_trajectory.append('')
    speed.append('')
    dirdegrees.append('')
    hazardtype.append('')
    hazardseverity.append('')
    airsigmet = {}
    modifiedtraj = [] 
    airsigmet = getairsigmetareas(input_dict['time'][i])
    for key in airsigmet:
        is_within_polygon = is_point_in_polygon(input_dict['sampled_trajectory'][i][0], input_dict['sampled_trajectory'][i][1], airsigmet[key]['area'])
        if is_within_polygon:
            modifiedtraj = check_typical_trajectory(resampled_datasets, airsigmet[key], i)
            if modifiedtraj:
		modified_trajectory[i] = modifiedtraj
		dirdegrees[i] = airsigmet[key]['movementdirdegrees']
		speed[i] = airsigmet[key]['movementspeedknots']
		hazardtype[i] = airsigmet[key]['hazardtype']
		hazardseverity[i] = airsigmet[key]['hazardseverity']
            else:
		if not isinstance(modified_trajectory[i], list):
			modified_trajectory[i] = '-'	
		if dirdegrees[i] == '':
			dirdegrees[i] = '-'	
		if speed[i] == '':
			speed[i] = '-'	
		if hazardtype[i] == '':
			hazardtype[i] = '-'
		if hazardseverity[i] == '':
			hazardseverity[i] = '-'
		
        else:
		if not isinstance(modified_trajectory[i], list):
			modified_trajectory[i] = '-'	
		if dirdegrees[i] == '':
			dirdegrees[i] = '-'	
		if speed[i] == '':
			speed[i] = '-'	
		if hazardtype[i] == '':
			hazardtype[i] = '-'
		if hazardseverity[i] == '':
			hazardseverity[i] = '-'

for i in range(0, len(input_dict['sampled_trajectory'])):
	if modified_trajectory[i] != '-':
		print input_dict['sampled_trajectory'][i][0], input_dict['sampled_trajectory'][i][1], ":   Modified:  ", modified_trajectory[i], "  Speed: ", speed[i], "  Movement Degrees:  ", dirdegrees[i], "  Hazard Type:  ", hazardtype[i], "   Hazard Severity: ", hazardseverity[i]
	else:
		print input_dict['sampled_trajectory'][i][0], input_dict['sampled_trajectory'][i][1]
	print	























