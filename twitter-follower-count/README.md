Projects executed on AWS EMR 
------------
### Dataset: 
http://socialcomputing.asu.edu/datasets/Twitter <br />
The dataset contains two files: **nodes.csv** or the users and **edges.csv** which contains entries in the form of (user, user-it-follows) <br />
Number of users : **11 million** <br />
Number of relationships : **85 million** <br />

Project 1: Max Filter
--------------
### Approach:
```
// First MapReduce job : find follower counts
map(line l):
  split (user, user-it-follows) pair in l on “,”
  if user-it-follows exists:
    emit(user-it-follows,1)

reduce(user x, [c1,c2…]): // where c1,c2 are the counts for x
  count=0
  for c in [c1,c2….]:
    count+=c
  emit(x, count)

// Main MaxFilter Map-only job:
maxfilterMapper(line)
  read MAX value from context
  followers = split (user, followerCount) pair in line on “,”
  if( followers < MAX)
    emit(users[0],users[1])
```
### Analysis
A filter is useful to trim your data set. For example, in project-2, I am finding out the social triangles, so I can use max filter to reject users with followers below a certain threshold. Every user must have atleast  one follower to find if it is part of a social triangle

Project 0: Finding number of followers for each user
-----------------
### Approach:
```
map(line l):
  split (user, user-it-follows) pair in l on “,”
  if user-it-follows exists:
    emit(user-it-follows,1)

reduce(user x, [c1,c2…]): // where c1,c2 are the counts for x
  count=0
  for c in [c1,c2….]:
    count+=c
  emit(x, count)
```

### Execution:
- The program was run on AWS on two clusters
- Cluster-1 - 1 master and 5 workers (all m4.large machines)
- Cluster-2 - 1 master and 10 workers (all m4.large machines)
- 2 independent runs were made on each cluster

### Results:
<table>
    <thead>
      <tr>
        <th></th>
        <th>Runtime-1</th>
        <th>Runtime-2</th>
      </tr>
    </thead>
    <tbody>
        <tr>
          <td>Cluster-1</td>
          <td>1 minute and 54 seconds</td>
          <td>1 minute and 56 seconds</td>
        </tr>
        <tr>
          <td>Cluster-2</td>
          <td>1 minute and 17 seconds</td>
          <td>1 minute and 16 seconds</td>
        </tr>
       <tr>
          <td>Speedup</td>
          <td>1.48</td>
          <td>1.526</td>
        </tr>
    </tbody>
</table>
Speed up is calculated by run time on cluster-1 vs run time on cluster-2

