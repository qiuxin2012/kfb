import redis
from utils import settings, streaming_image_producer
import os
import cv2
import json
import uuid
import time
from os import listdir
from os.path import isfile, join


DB = redis.StrictRedis(host=settings.REDIS_HOST,
                       port=settings.REDIS_PORT, db=settings.REDIS_DB)

DB.rpush("count-kfb", "T1|30102")

for root, _, files in os.walk('/tmp/check_img'):
    for file in files:
        assert file.endswith('jpg')

        region = cv2.imread(os.path.join(root, file))
        region = cv2.imencode(".jpg", region)[1]
        fname = "T1/"+file
        # print(fname)
        streaming_image_producer.image_enqueue(fname, region)
