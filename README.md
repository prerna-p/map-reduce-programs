# Map Reduce Framework

### Objective: 
Utilizing distributed systems (HDFS) to execute computations on a large datasets 

### The dataset
http://socialcomputing.asu.edu/datasets/Twitter <br />
The dataset contains two files: **nodes.csv** or the users and **edges.csv** which contains entries in the form of (user, user-it-follows) <br />
Number of users : **11 million** <br />
Number of relationships : **85 million** <br />

_Note: The analysis for these projects are performed solely by me, Prerna Purohit. Do not make an attempt to duplicate this information as is, or in a highly similar manner. Please use the contents of this repo as a reference only._

### Project 0 : Finding number of followers for each user:
 - [Source](https://github.com/prerna-p/map-reduce-programs/blob/master/twitter-follower-count/src/main/java/followercounts/FollowerCount.java)
 - [Analysis](https://github.com/prerna-p/map-reduce-programs/tree/master/twitter-follower-count#project-0-finding-number-of-followers-for-each-user)

### Project 1 : Filtering out users having followers less than MAX_FILTER value taken as input:
 - [Source](https://github.com/prerna-p/map-reduce-programs/blob/master/twitter-follower-count/src/main/java/joins/MaxFilter.java)
 - [Analysis](https://github.com/prerna-p/map-reduce-programs/tree/master/twitter-follower-count#project-1-max-filter)

 ### Project 2 : Finding social triangles:
 - [Source](https://github.com/prerna-p/map-reduce-programs/tree/master/twitter-follower-count/src/main/java/joins)
 - [Analysis](https://github.com/prerna-p/map-reduce-programs/blob/master/twitter-follower-count/src/main/java/joins/JoinsREADME.md)

  ### Project 3 : Utilizing KMeans to aggregate users based on number of followers:
 - [Source](https://github.com/prerna-p/map-reduce-programs/tree/master/twitter-follower-count/src/main/java/kmeans)
 - [Analysis](https://github.com/prerna-p/map-reduce-programs/blob/master/twitter-follower-count/src/main/java/kmeans/KmeansREADME.md)
