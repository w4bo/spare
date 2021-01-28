# TrajectoryMining
This project is about discovering General Co-movement Pattern from Large-scale Trajectories.

I am not longer supporting this project as I have dismissed from previous affliation. For request, please email fanqinus@gmail.com 

#How to run this project
Two tasks are available: _snapshotGeneratorJar_ which build a fat jar
based on _Spark 2.11_ to execute the **Snapshot Generation** phase
and _spareJar_ that takes care of the **Star Graph generation** and
**Apriori Enumeration**.
Two mocks dataset are included inside the `src/main/resources` folder.

##Running Snapshots generation

Simply launch the following command:

```
spark-submit Trajectory-Instrumentally-project-x.x.x-clustering.jar \
--input_file=hdfs/input/files/path \
--output_dir=/hdfs/output/files/path \ 
--epsilon=X \
--min-points=Y \
--gcmpm=Z \
--snapshot-partition=Q \
--numexecutors=E \
--numcores=C \
--executormemory=5g \
--debug=OFF
```

where: 
   * `--input_file=hdfs/input/files/path` is the absolute path of the input file or folder
   * `--output_dir=/hdfs/output/files/path` is the absolute path of the output folder
   * `--epsilon=X` is the epsilon value for the DBSCAN algorithm
   * `--min-points=Y` is the minPoints value for DBSCAN
   * `--gcmpm=Z` is the **M** value for GCMP, used for pruning.
   * `--snapshot-partition=Q` is the number of partition of the output.
   * `--numexecutors=E` is the number of executors used by _Spark_
   * `--numcores=C` is the number of cores per executor used by _Spark_
   * `--executormemory=5g` is the memory allocated for each executor 
   * `--debug=OFF` activate or disable _Spark_ logs
   
##Running SPARE

Simply launch the following command:

```
spark-submit Trajectory-Instrumentally-project-x.x.x-spare.jar \
--input_dir=hdfs/input/files/path \
--output_dir=/hdfs/output/files/path \ 
--gcmp_m=M \
--gcmp_k=K \
--gcmp_l=L \
--gcmp_g=G \
--input_partitions=i \
--numexecutors=E \
--numcores=C \
--executormemory=5g \
--debug=OFF
```

where: 
   * `--input_file=hdfs/input/files/path` is the absolute path of the input file or folder
   * `--output_dir=/hdfs/output/files/path` is the absolute path of the output folder
   * `--gcmp_m=M` is the **M** value for GCMP, used for pruning.
   * `--gcmp_k=K` is the **K** value for GCMP, used for pruning.
   * `--gcmp_l=L` is the **L** value for GCMP, used for pruning.
   * `--gcmp_g=G` is the **G** value for GCMP, used for pruning.
   * `--input_partitions=I` is the number of partition of the output.
   * `--numexecutors=E` is the number of executors used by _Spark_
   * `--numcores=C` is the number of cores per executor used by _Spark_
   * `--executormemory=5g` is the memory allocated for each executor 
   * `--debug=OFF` activate or disable _Spark_ logs