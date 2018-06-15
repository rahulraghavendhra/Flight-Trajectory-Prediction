#! /usr/bin/python  
from math import sqrt, pow  
import csv
import re
class DBSCAN:  
 #Density-Based Spatial Clustering of Application with Noise -> http://en.wikipedia.org/wiki/DBSCAN  
    def __init__(self):  
#        self.name = 'DBSCAN'  
        self.DB = [] #Database  
        self.esp = 0.02 #neighborhood distance for search  
        self.MinPts = 2 #minimum number of points required to form a cluster  
        self.cluster_inx = -1  
        self.cluster = []  
        self.centroid = []
        self.flightplanPoint = ""
       
    def DBSCAN(self):  
      for i in range(len(self.DB)):  
        p_tmp = self.DB[i]  
        if (not p_tmp.visited):  
         #for each unvisited point P in dataset  
          p_tmp.visited = True  
          NeighborPts = self.regionQuery(p_tmp)  
          if(len(NeighborPts) < self.MinPts):  
           #that point is a noise  
            p_tmp.isnoise = True  
 #           print p_tmp.show(), 'is a noise'  
          else:  
            self.cluster.append([])  
            self.cluster_inx = self.cluster_inx + 1  
            self.expandCluster(p_tmp, NeighborPts)     
       
    def expandCluster(self, P, neighbor_points):  
      self.cluster[self.cluster_inx].append(P)  
      iterator = iter(neighbor_points)  
      while True:  
        try:   
          npoint_tmp = iterator.next()  
        except StopIteration:  
         # StopIteration exception is raised after last element  
          break  
        if (not npoint_tmp.visited):  
          #for each point P' in NeighborPts   
          npoint_tmp.visited = True  
          NeighborPts_ = self.regionQuery(npoint_tmp)  
          if (len(NeighborPts_) >= self.MinPts):  
            for j in range(len(NeighborPts_)):  
              neighbor_points.append(NeighborPts_[j])  
        if (not self.checkMembership(npoint_tmp)):  
         #if P' is not yet member of any cluster  
          self.cluster[self.cluster_inx].append(npoint_tmp)  
#        else:  
#          print npoint_tmp.show(), 'is belonged to some cluster'  
   
    def checkMembership(self, P):  
     #will return True if point is belonged to some cluster  
      ismember = False  
      for i in range(len(self.cluster)):  
        for j in range(len(self.cluster[i])):  
          if (P.flightid == self.cluster[i][j].flightid): #RAG
            ismember = True  
      return ismember  
       
    def regionQuery(self, P):  
   #return all points within P's eps-neighborhood, except itself  
      pointInRegion = []  
      for i in range(len(self.DB)):  
        p_tmp = self.DB[i]  
        if (self.dist(P, p_tmp) < self.esp and P.flightid != p_tmp.flightid): #RAG 
          pointInRegion.append(p_tmp)  
      return pointInRegion  

    def identifycluster(self):
        for i in range(len(self.cluster)):
            if(self.dist(self.flightplanPoint, self.centroid[i]) < 0.2):
                self.flightplanPoint.belongstoCluster.append(i);

    def showCluster(self):
        if len(self.flightplanPoint.belongstoCluster) == 0 :
            print "The input historical flight trajectory does not belong to any cluster"
        else:
            outputfile_handle = open('typicaltrajectory-flightid.csv', 'w')
            print "The input historical trajectory belongs to cluster", ' '.join(str(self.flightplanPoint.belongstoCluster))
            for clusterid in self.flightplanPoint.belongstoCluster:
                for j in range(len(dbScan.cluster[clusterid])):  
                    outputfile_handle.write(str(self.cluster[clusterid][j].getFlightID()))
                    outputfile_handle.write('\n')
            outputfile_handle.close()

   
    def dist(self, p1, p2):  
   #return distance between two point  
      dx = (p1.x - p2.x)  
      dy = (p1.y - p2.y)  
      return sqrt(pow(dx,2) + pow(dy,2))  

   
class Point:  
    def __init__(self, x = 0, y = 0, flightid = 0, visited = False, isnoise = False):  
      self.x = x  
      self.y = y  
      self.flightid = flightid
      self.visited = False  
      self.isnoise = False  
      self.belongstoCluster = []
   
    def show(self):  
      return self.flightid  

    def show1(self):  
      return self.x,self.y  
    
    def getX(self):
        return self.x

    def getY(self):
        return self.y

    def getFlightID(self):
        return self.flightid


if __name__=='__main__':  
  
#    vecPoint = [Point(11,3), Point(10,4), Point(11,5), Point(12,4), Point(13,5), Point(12,6), Point(6,10), Point(8,10), Point(5,12), Point(7,12)]  
    vecPoint = []
    flightplan = []
    centroidX = []
    centroidY = []
    
    dataPath = '/Volumes/Seagate Backup Plus Drive/KDD/Working_Directory/data.csv'
    inputPath = '/Volumes/Seagate Backup Plus Drive/KDD/Working_Directory/input.csv'
    with open(dataPath,'rb') as f:
        reader = csv.reader(f)
        for row in reader:
            vecPoint.append(Point(float(row[1]),float(row[2]), int(row[0])))

    with open(inputPath,'rb') as f1:
        reader = csv.reader(f1)
        for row in reader:
            flightplan.append((float(row[0]),float(row[1])))

   #Create object  
    dbScan = DBSCAN()  
   #Load data into object  
    dbScan.DB = vecPoint;  
   #Do clustering  
    dbScan.DBSCAN()  
   #Show result cluster  
    for i in range(len(dbScan.cluster)):
        print 'Cluster: ', str(i)
        sumx = 0
        sumy = 0
        for j in range(len(dbScan.cluster[i])):  
            sumx += dbScan.cluster[i][j].getX()
            sumy += dbScan.cluster[i][j].getY()
            print dbScan.cluster[i][j].show()
        centroidX.append(sumx/float(len(dbScan.cluster[i])))
        centroidY.append(sumy/float(len(dbScan.cluster[i])))

    # calculate the centroid of the clusters
    for i in range(len(centroidY)):
        print 'Centroid ', i
        dbScan.centroid.append(Point(float(centroidX[i]), float(centroidY[i])))
        print dbScan.centroid[i].show1()

    # calculate the average of the flightplan
    distance = tuple(map(lambda y: sum(y) / float(len(y)), zip(*flightplan)))
    dbScan.flightplanPoint = Point(float(distance[0]),float(distance[1]))

    dbScan.identifycluster()
    dbScan.showCluster()
