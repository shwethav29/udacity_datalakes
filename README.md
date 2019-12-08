# SPARKIFY

A music streaming startup, Sparkify, has grown their user base and song database and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Purpose

The purpose of this project is to build ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

####> Steps to follow.
>>1 Create a S3 "s3a://shwe-uda-dl-spark-project/" in US-west coast oregon.

>>2 Build a ETL pipeline using spark, to process the data from S3 and write back to S3.

>>3 Create a EMR cluster on Amazon. See instruction to Create Amazon EMR.

>>4 Update dl.cfg with AWS KEY and SECRET

>>5 Create the keypair see instructions below to Create KEY PAIR

>>6 Transfer etl.py and dl.cfg to EMR using scp 

scp -i ~/amazon_credentials/spark-EMR-spark-cluster-keypair/spark-cluster.pem dl.cfg hadoop@ec2-52-38-66-241.us-west-2.compute.amazonaws.com:/home/hadoop


scp -i ~/amazon_credentials/spark-EMR-spark-cluster-keypair/spark-cluster.pem etl.py hadoop@ec2-52-38-66-241.us-west-2.compute.amazonaws.com:/home/hadoop

>>7 ssh to the EMR machine

ssh -i ~/amazon_credentials/spark-EMR-spark-cluster-keypair/spark-cluster.pem  hadoop@ec2-34-221-200-106.us-west-2.compute.amazonaws.com

>>8 Using sudo, append the following line to the /etc/spark/conf/spark-env.sh file:

export PYSPARK_PYTHON=/usr/bin/python3

>>9 Submit spark job

spark-submit --master yarn etl.py
(This job will take a very long time to complete based on the data you are running. You will see executors killed by the driver, thats ok! Wait the job will complete.)

>>10 S3 link for project Data

>>>>Song data: s3://udacity-dend/song_data

>>>>Log data: s3://udacity-dend/log_data

##Instructions to Create KEY PAIR

>>>1 Goto EC2 service on AWS

>>>2 Under Network Security -> Key Pairs

>>>3 Give name spark-cluster

>>> A pem file should download to your local machine save that file in a folder on your local machine.

## Instructions to create EMR cluster on Amazon

>>>1 Go to the Amazon EMR Console

>>>2 Select Cluster in the menu left, and Click Create Cluster.

>>>3 Configure cluster with following settings:
Release: emr-5.20.0 or later
Applications: Spark: Spark 2.4.0 on Hadoop 2.8.5 YARN with Ganglia 3.7.2 and Zeppelin 0.8.0
Instance type: m3.xlarge
Number of instance: 3
EC2 key pair: Use the Spark-cluster we created in step 5

>>>4 Rest all keep default and create cluster.

>>>5 Now click on SSH Next to Master Public DNN. Follow instructions to SSH.
If timed out then go to security groups select Master Click Inbound ->EDIT
If SSH is present delete and add Rule.
SSH->TCP - 22 select My IP ->Save
SSH - Error - Unprotected file
chmod 600 spark-cluster.pem

### >**ETL Pipeline**
>>> 1.The data from the S3 is processed from by SPARK the song_data is loaded to the songs and artist parquet files.
>>> 2.The song data is partitioned by year and artist_id, the artist data is partitioned by artist id. 
>>> 3.The data from the log_data is loaded to users, time and songplay.
>>> 4.The time table is created using the ts column from the log_data. The time data is partitioned be year and month.
>>> 5.song_plays is partitioned by year and month. 


### sample dl.cfg
[AWS]
KEY=
SECRET=
