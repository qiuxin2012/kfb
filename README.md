# KFBio project

## Prerequisites
* Analytics zoo
* cpp (kfbReader, etc) provided
* Spark 2.4.3 +
* Redis

## Steps to run
start redis

`python py/monitor.py` to prepare for output

`spark-submit --class com.intel... --modelPath ...` to start streaming

`python py/cut_kfb_image.py` to cut KFB image into pieces
