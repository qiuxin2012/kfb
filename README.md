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

## Issue notes
* Thread limit: for 16 threads cutting image error, reduce thread number to 8, this may due to `ReadRoiData` limitation of `.so` code

