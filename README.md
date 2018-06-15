In this paper, the semi-lazy data mining paradigm is studied and implemented to predict the trajectory of aircraft in-
flight. A clustering algorithm is applied on the historical radar trajectory data to abstract a set of typical trajectories for
the given source and destination airports. The typical trajectories, which are a subset of the historical data are now
analysed using a intent-based model which includes dynamically changing weather conditions. The input flight plan is
subjected to the given weather conditions and any conflicts are resolved by suggesting alternate route or deviation
from the current flight path, obtained from the output of the intent based model.
Install the following packages:
===============================
(i) MongoDB
(ii) python 3.0
(iii) python libraries-
	a)numpy b)scipy c)pandas d)matplotlib e)networkx f)pymongo


Input Query:
=============
(i) departure airport, arrival airport,time range [Eg: SFO,LAX,time]
(ii) input.csv => Flight plan of input trajectory
(iii) airsigmet.csv => Weather information

Run :
=====

(i) To get all historical radar trajectories from db  :
===================================================
python get_all_trajectories.py

(ii) To run clustering for the given input query:
================================================
python dbscan.py > clustering_results.txt

(iii) To get typical trajectories from cluster:
===============================================
python get_typical_trajectories.py

(iv) Implementation to get predicted/recommended trajectory :
============================================================
python sampling.py > results.txt

Output :
========
clustering_results.txt
results.txt
