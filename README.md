# hdfs-tools

## print_block_replica_status
Display information on Namenode's primary data structures
  + Script usage
    ```shell
    print_block_replica_status -h
    usage: print_block_replica_status [-h] [-m METASAVE_PATH] {block,count} ..

    Get information on Namenode\'s primary data structures.

    positional arguments:
      {block,count}
        block               print blocks status
        count               print blocks count

    optional arguments:
      -h, --help            show this help message and exit
      -m METASAVE_PATH, --metasave-path METASAVE_PATH
                            the path to the metasave file

    print_block_replica_status count -h
    usage: print_block_replica_status count [-h] [-l LIVE] [-d DECOMMISSIONED]
                                            [-c CORRUPT] [-e EXCESS]

    optional arguments:
      -h, --help            show this help message and exit
      -l LIVE, --live LIVE  the live replicas count to match
      -d DECOMMISSIONED, --decommissioned DECOMMISSIONED
                            the decommissioned replicas count to match
      -c CORRUPT, --corrupt CORRUPT
                            the corrupt replicas count to match
      -e EXCESS, --excess EXCESS
                            the excess replicas count to match

    print_block_replica_status block -h
    usage: print_block_replica_status block [-h] (-n DATANODE | -u DATANODE)

    optional arguments:
      -h, --help            show this help message and exit
      -n DATANODE, --no-live-replicas DATANODE
                            print blocks with no live replicas on decommissioned
                            datanode
      -u DATANODE, --under-replicated DATANODE
                            print under replicated blocks on decommissioned
                            datanode
    ```
  + Investigate decommission issue / monitor decommission process
    + generate metasave data on active NameNode in prod:
      + login as hdfs user on active NameNode
      + generate metasave data:
        ```shell
        hdfs dfsadmin -metasave hadoop-hdfs-metasave.log
        ```
      + check the file content at /opt/hadoop/logs/hadoop-hdfs-metasave.log
        + Each block will have a line such as above which contains information on its replication:
          ```
          l: live replicas
          d: decomissioned replicas
          c: corrupt replicas
          e: excess replicas
          ```
    + if there are lots of under-replicated blocks:
      + execute the command `print_block_replica_status`, e.g.:
        ```shell
        # print the number of blocks with no live replicas (i.e. the only replica is on a decommissioned datanode)
        print_block_replica_status -m hadoop-hdfs-metasave.log count -l 0 -d 1
        dev-hdfs-11.example.com --> 3

        # print the files which block(s) are only on dev-hdfs-11.example.com
        print_block_replica_status -m hadoop-hdfs-metasave.log block -n dev-hdfs-11.example.com
        /user/user1/dev/data_dir/fr/2022/202206/20220601/stages/0_pipeline_e2fa68ed49ca/stages/4_strIdx_6be0ae25bfeb/metadata/part-00000
        /user/user1/dev/data_dir/fr/2022/202206/20220601/stages/1_pipeline_01f51e379468/stages/0_xgbr_c26c29366f1b/data/XGBoostRegressionModel
        /user/user1/qa/data_dir/fr/2022/202206/20220601/stages/0_pipeline_e2fa68ed49ca/stages/3_strIdx_d84dc938a160/data/part-00000-8f26cae2-7f71-4f2a-820b-a916263e497e-c000.gz.parquet

        ```
## get_hadoop_site_keys
Get keys from hadoop XML configuration files and sort them for easier comparison
  + Script usage
    ```shell
    usage: get_hadoop_site_keys [-h] xml_path

    Get keys from hadoop site XML files.

    positional arguments:
      xml_path    the path to the hadoop site XML file

    options:
      -h, --help  show this help message and exit
    ```
  + Example usage: get keys from main hadoop configuration XML files on a server
    ```shell
    HOSTNAME=dev-spark-01.example.com
    mkdir -p ${HOSTNAME%%.*}
    for f in core-site.xml hdfs-site.xml mapred-site.xml yarn-site.xml; do ./get_hadoop_site_keys <(ssh $HOSTNAME cat /opt/hadoop/conf/$f) > ${HOSTNAME%%.*}/$f; done

    ```
  + Generated files
    + dev-spark-01/core-site.xml
      ```
      fs.defaultFS: hdfs://dev-cluster
      ```
    + dev-spark-01/hdfs-site.xml
      ```
      dfs.client.failover.proxy.provider.dev-cluster: org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider
      dfs.datanode.max.transfer.threads: 8192
      dfs.ha.namenodes.dev-cluster: nn1,nn2
      dfs.namenode.avoid.read.stale.datanode: true
      dfs.namenode.avoid.write.stale.datanode: true
      dfs.namenode.http-address.dev-cluster.nn1: dev-hdfs-01.example.com:50070
      dfs.namenode.http-address.dev-cluster.nn2: dev-hdfs-02.example.com:50070
      dfs.namenode.rpc-address.dev-cluster.nn1: dev-hdfs-01.example.com:9000
      dfs.namenode.rpc-address.dev-cluster.nn2: dev-hdfs-02.example.com:9000
      dfs.namenode.shared.edits.dir: qjournal://dev-hdfs-01.example.com:8485;dev-hdfs-02.example.com:8485;dev-hdfs-03.example.com:8485/dev-cluster
      dfs.nameservices: dev-cluster
      dfs.permissions.enabled: false
      dfs.replication: 1
      ```
    + dev-spark-01/mapred-site.xml
      ```
      mapreduce.framework.name: yarn-tez
      mapreduce.job.emit-timeline-data: true
      mapreduce.job.queuename: spark-batch
      mapreduce.jobtracker.staging.root.dir: hdfs:/user
      ```
    + dev-spark-01/yarn-site.xml
      ```
      yarn.nodemanager.aux-services.spark_shuffle.class: org.apache.spark.network.yarn.YarnShuffleService
      yarn.nodemanager.aux-services: mapreduce_shuffle,spark_shuffle
      yarn.nodemanager.resource.detect-hardware-capabilities: true
      yarn.resourcemanager.hostname: dev-yarn-01.example.com
      yarn.timeline-service.enabled: true
      yarn.timeline-service.generic-application-history.enabled: true
      ```
