Projects executed on AWS EMR 
------------
### Dataset: 
http://socialcomputing.asu.edu/datasets/Twitter <br />
The dataset contains two files: **nodes.csv** or the users and **edges.csv** which contains entries in the form of (user, user-it-follows) <br />
Number of users : **11 million** <br />
Number of relationships : **85 million** <br />

Project 2: Social Triangles
--------------
### Introduction: 
A social triangle is a scenario when User A, B, C exist in the system and A follows B, B follows C and C follows A. So there exists a social _amplifier_ triangle as shown below:<br /> 
<img src="https://github.com/prerna-p/map-reduce-programs/blob/master/img/SocialTriangle.jpg"></img>
The **goal** of the problem is to count the number of social triangles for a trimmed version of the dataset (i.e users having followers above a certain threshold)<br />
The way to solve this computation is to first find the two length paths A->B->C by joining the data to itself on the attribute B. Then second, find the closing paths C->A for each two length path.<br />

### Approach 1: Replicated Join
```
load max filter output to hdfs cache
setup()
  H = new HashMap
  for each line in hdfs cache
    H.insert(line[0],line[1])

map(line)
  toNodes = H.get(line[1])
  for each s in toNodes
    sList = H.get(s);
    for each x in sList
      if(x.equals(nodes[0]))
        increment global counter
```
### Approach 2: RS Join
```
// STEP ONE
tokenizerMapper(line)
  user,followee = line.split(“,”)
  emit(user,(user,followee,FROM))
  emit(followee,(user,followee,TO))
  
listReducer(key, [val1,val2….])
  for each val in [val1,val2….]
    r = val.split(",");
    if(r[2].equals("to"))
      add r[0],r[1] to to_list
    else
      add r[0],r[1] to from_list
    for each f in from_list
      for each t in to_list
        if f[1].equals(t[0])
          emit((f+t[1]),null);

//STEP-2
fromMapper(line)
  user,followee = line.split(“,”)
  emit( user,(user,followee,FROM))

stepTwoMapper(two-paths)
  for each record (start,mid,end), split by comma
    emit(end,(start,end,TO))
  
triangleCountReducer(key, [val])
  split [val] into to_list and from_list
  for each f in from_list
    for each t in to_list
      if f[1].equals(t[0]) AND f[0].equals(t[1])
        increment global counter
```
### Results:
<table>
    <thead>
      <tr>
        <th></th>
        <th>Computation</th>
        <th>6 m4.large machines</th>
        <th>11 m4.large machines</th>
        <th>Follower threshold</th>
        <th>Social triangle count</th>
      </tr>
    </thead>
    <tbody>
        <tr>
          <td>RSJoin</td>
          <td>23 minutes and 20 seconds</td>
          <td>39 minutes and 19 seconds</td>
          <td>20000</td>
          <td>2411611</td>
        </tr>
        <tr>
          <td>Replicated join</td>
          <td>18 minutes and 54 seconds</td>
          <td>18 minutes</td>
          <td>45000</td>
          <td>5852141</td>
        </tr>
    </tbody>
</table>

