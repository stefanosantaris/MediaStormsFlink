# MediaStormsFlink

About

This is a Java implementation of the Media Storm Indexing algorithm, presented in "D. Rafailidis and S. Antaris, Indexing Media Storms on Flink”, IEEE International Conference on Big Data, 2015". The source code reproduces the experiments described in our paper.

Datasets

Our experimental evaluation was performed on two publicly available datasets of image descriptor vectors. We used the GIST-80M-348d dataset of the Tiny Image Collection and the SIFT-1B-128d of the TEXMEX collection, publicly available at http://horation.cs.nyu.edu/mit/tiny/data/index.html and http://corpus-texmex.irisa.fr, respectively. Each dataset file should contain an image descriptor vector per row and the dimensions of each descriptor should be comma separated.

Usage

Our proposed media storm indexing algorithm was implemented using the Apache Flink specifications to provide a distributed stream data processing mechanism. In our implementation, the HBase distributed data management system was used and the Apache Kafka service was applied for the queue services.

In our repository, we provide the source code of the proposed Media Storm Indexing algorithm. In order to run our code, the following steps are required:

1) download and install the Flink platform in Cluster Setup Deployment;

2) download and install the HBase in a distributed mode;

3) connect Flink with HBase by specifying the HBase cluster in conf/flink-conf.yaml;

4) download and install the Kafka service

5) connect Flink with Kafka by specifying the hostname in MediaStormIndexing.java;

6) build the Media Storm Indexing project by executing maven clean package in terminal;

7) Media Storm Indexing is now installed in build-target

8) run /{where_link_is_installed}/flink run ./{where_code_is_built}/MediaStormIndexing.jar