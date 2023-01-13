# Background:
URLCount works by using the base template of the map-reduce framework for wordCount but modified to find href tags that contain links.
This is done using Java pattern matching with REGEX of the form href=\".*\".
It then loops through each line of the input matching each link and stripping the "href=" off it.
Finally, the reducer counts up occurrences of individual links and limits the output to only links that occur more than five times.
Getting around Java/Hadoop combiner issues, there is a separate combiner function/class that does the same job as the reducer but without the more than five occurrences check.

To run this, you will need a system with Java and Hadoop installed.
First, prepare to download the files needed and set up the directory structure.
Then run make URLCount compile the java file.
Finally, run make runURL or time make runURL if you want to time the Hadoop job.
If running this in dataproc, ssh into the master node of the cluster, then run all the make commands after making a user directory in hdfs using hdfs dfs -mkdir hdfs://CLUSTERNAME-m/user/USERNAME.

# Observations:
The default behavior of Hadoop in Java is that the reducer is duplicated as the combiner on the cluster to improve efficiency and reduce the final number of counting and network operations the reducer has to perform.
However, when we add the "only output links that occur more than five times" logic, the reducer is no longer both commutative and associative and, therefore, cannot be used as the combiner.
For example, if one node has four occurrences and the other node has four occurrences of a specific link, each node would hit the > 5 logic, fail it individually, and throw out the links.
However, there would have been eight occurrences in the final reducer, and the link should have been kept.
This issue can be solved by writing a separate combiner function/class that is identical to the reducer, counts up occurrences, and does not do the "greater than five occurrences" checking logic.

# Results:
##### https://github.com/cu-csci-4253-datacenter-fall-2022/lab2-hadoop-url-lister-migh6544/tree/master/Screenshots

	*	2-node Cluster Time Output -> real 0m39.551s | user 0m13.296s | sys 0m0.733s

	*	4-node cluster Time Output -> real 0m42.976s | user 0m11.355s | sys 0m0.604s

# Conclusion:
The exciting part of this output is that the local run is much faster than both cluster runs and that the two are at almost the same speed, independent of the number of nodes.
This is likely because the clusters are optimized for substantial quantities of data.
So, when running it for small data sets like we are, the additional overhead of distributing the executables and files to all nodes adds time instead of reducing time like intended.
This is as opposed to the local run that does not need to do this distribution and thus is much faster at small data sizes but will slow down with increased amounts of data to process as it is limited by the local number of cores in the CPU.
The cluster runs would speed up as you increase the amount of data being processed, as it can use many more cores than would otherwise be possible locally.
