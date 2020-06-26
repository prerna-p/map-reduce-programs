# Map Reduce Framework

### Objective: 
Utilizing distributed systems (HDFS) to execute computations on a large datasets 

### The dataset
http://socialcomputing.asu.edu/datasets/Twitter <br />
The dataset contains two files: **nodes.csv** or the users and **edges.csv** which contains entries in the form of (user, user-it-follows)
Number of users : 11 million <br />
Number of relationships : 85 million <br />

### Project 0 : Finding number of followers for each user:
 - [Source](https://github.com/prerna-p/map-reduce-programs/blob/master/twitter-follower-count/src/main/java/followercounts/FollowerCount.java)
 - [Analysis]()

### Project 1 : Filtering out users having followers less than MAX_FILTER value taken as input:
 - [Source](https://github.com/prerna-p/map-reduce-programs/blob/master/twitter-follower-count/src/main/java/joins/MaxFilter.java)
 - [Analysis]()

 ### Project 2 : Finding social triangles:
 - [Source](https://github.com/prerna-p/map-reduce-programs/tree/master/twitter-follower-count/src/main/java/joins)
 - [Analysis]()

  ### Project 3 : Utilizing KMeans to aggregate users based on number of followers:
 - [Source](https://github.com/prerna-p/map-reduce-programs/tree/master/twitter-follower-count/src/main/java/kmeans)
 - [Analysis]()
