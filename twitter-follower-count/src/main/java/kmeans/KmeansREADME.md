Projects executed on AWS EMR 
------------
### Dataset: 
http://socialcomputing.asu.edu/datasets/Twitter <br />
The dataset contains two files: **nodes.csv** or the users and **edges.csv** which contains entries in the form of (user, user-it-follows) <br />
Number of users : **11 million** <br />
Number of relationships : **85 million** <br />

Project 2:  Utilizing [KMeans clustering](https://en.wikipedia.org/wiki/K-means_clustering) to aggregate users based on number of followers:
--------------
**Goal:** <br />
Cluster twitter users using the difference in number of followers between 2 users as the distance measure. For instance, consider a user with 10 followers and user with 18 followers, then their distance is 8. On termintation, each cluster should contain users with a similar number of followers. <br />

**Key Aspects:** <br />
Determining the good start centers for the algorithm. Using pandas, I analyzed the distribution of the dataset and the mean, standard deviation, percentiles and maximum follower value in the dataset. The values are in the tables below. Based on these values, I picked initial centers as: <br />
- 1 because 75% of data has #followers=1,
- 10 as mean #followers is 12.87,
- 500 as standard deviation is 537.8,
- 500000 though maximum value of followers is 564512, but as figure-2 shows, it can be considered as outlier.

### Approach: 
Psuedocode

```java
// First job: find follower counts
map(line l):
  split (user, user-it-follows) pair in l on “,”
  if user-it-follows exists:
    emit(user-it-follows,1)
reduce(user x, [c1,c2…]):
  count=0
  for c in [c1,c2….]:
    count+=c
  emit(x,count)

// Second job: K-Means algorithm, run iteratively from driver()
initialize global counter for SSE
// Map Phase
Mapper {
  setup():
    read centroids from cache
    store all centroids in global centerList collection
  
  // helper function
  distance(a, b):
    return Math.abs(a-b)
  
  map(line):
    closestCenter = centerList.get(0)
    minDist = distance(closestCenter,followers)
    for(c in centerList){
      if (distance(c,followers) < minDist){
        closestCenter = c
        minDist = distance(closestCenter,followers)
      }
    }
    emit(closestCenter,line)

  cleanup():
    for(c in centerList)
      emit(c,“dummy”)
}
// Reduce Phase
Reducer {
  initialize global errList collection
  // helper function
  errors(mi, x):
    sq = (mi - x) * (mi - x)
    return sq

  reduce(key, Iterable valueList):
    initialize updatedCenter,counts,sse1 to 0
    M = key
    for(tuple in valueList){
      if(!tuple.equals("dummy"))
        updatedCenter += (extracted followers from tuple)
        counts++
      if(updatedCenter == 0) updatedCenter = M //for centroids that did not get updated
      else
        updatedCenter = updatedCenter/counts
        //calculate sse
        for(each followerCount from valueList)
          sse1 += errors(updatedCenter,followerCount) //used helper for sse
        
      errList.add(sse1)
      emit(updatedCenter,null)
    
    cleanup():
      sum = 0
      for(err in errList) sum = sum + err
        set global counter = sum
}
```

### Results:
Due to the size of the datasets, computation was run for 10 iterations only, to prevent raising the costs on AWS EMR
<table>
    <thead>
      <tr>
        <th>Configuration</th>
        <th>6 m4.large machines</th>
        <th>11 m4.large machines</th>
        <th>SSE</th>
      </tr>
    </thead>
    <tbody>
        <tr>
          <td>Good start</td>
          <td>8 minutes and 31 seconds</td>
          <td>7 minutes and 51 seconds</td>
          <td>378930692096</td>
        </tr>
        <tr>
          <td>Bad start</td>
          <td>8 minutes and 40 seconds</td>
          <td>7 minutes and 53 seconds</td>
          <td>549991570944</td>
        </tr>
    </tbody>
</table>