import argparse
from pyspark import SparkContext

import time
import os


pos_neg_threshold = 0.5
kfb_pos_ratio = 0.01


def prob_to_pos(line):
    # print(rec)
    # assert len(rec) == 3
    # print(line)
    rec = line.split("|")
    prob = float(rec[1])
    assert len(rec) == 3
    if prob <= pos_neg_threshold:
        return rec[0], 1, 1
    else:
        return rec[0], 0, 1


def count_pos(rec1, rec2):
    kfb1, kfb2 = rec1[0].split("/")[0], rec2[0].split("/")[0]

    assert len(rec1) == 3 and len(rec2) == 3
    assert kfb1 == kfb2
    # if rec1[0] != rec2[0]:
    #     print(rec1[0], rec2[0])
    #     time.sleep(60)
    #     assert rec1[0] == rec2[0]

    return rec1[0], rec1[1] + rec2[1], rec1[2] + rec2[2]


def get_result(rec):
    '''
    :param rec: rdd of (path, pos_possibility, neg_possibility)
    :return: positive 1, negative 0
    '''
    res = rec.map(prob_to_pos).reduce(count_pos)
    ratio = float(res[1]) / float(res[2])
    print("positive pieces ", res[1], "  total pieces ", res[2])

    kfb_name = res[0].split("/")[0]

    if ratio > kfb_pos_ratio:
        ans = kfb_name + " ratio is " + str(ratio) + " result is positive"
        print(ans)
    else:
        ans = kfb_name + " ratio is " + str(ratio) + " result is negative"
        print(ans)
    with open('/tmp/233.txt', 'a+') as f:
        f.write(ans + '\n')
    return


from py4j.protocol import Py4JJavaError
def path_exist(path):
    try:
        rdd = sc.textFile(path)
        rdd.take(1)
        return True
    except Py4JJavaError as e:
        return False


if __name__ == "__main__":

    sc = SparkContext()
    # a = sc.parallelize([('a', 0.3, 0.7), ('a', 0.2, 0.8), ('a', 1.0, 0.0)])
    #
    # print (a.count())
    #
    # print (a.count())
    #
    # b = get_result(a)
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger(__name__)

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', help="text file path, usually hdfs://")
    parser.add_argument('--threshold', help="threshold to judge small piece pos or neg")
    parser.add_argument('--ratio', help="ratio to judge kfb image pos or neg")
    args = parser.parse_args()
    file_path = args.file_path

    import redis
    from utils.helpers import settings

    DB = redis.StrictRedis(host=settings.REDIS_HOST,
                           port=settings.REDIS_PORT, db=settings.REDIS_DB)
    DB.flushall()
    # DB.lpush('count-kfb', ("dfsdf|adff"))
    last_name = None  # handle dead record
    cnt = 0
    while 1:
        while DB.llen('count-kfb') > 0:
            time.sleep(10)
            kv = DB.lpop('count-kfb').decode().split("|")

            # print(rec.decode())
            fname, total_count = kv[0], kv[1]

            if last_name == fname:
                cnt += 1
                if cnt > 30:
                    print("detected no file written to hdfs through 300 secs, skipped")
                    continue
            else:
                cnt = 1
            last_name = fname

            DB.lpush('count-kfb', (fname + "|" + total_count))
            total_count = int(total_count)

            dir_path = os.path.join(file_path, fname)
            print("file name is -> ", fname, "total_count is -> ", total_count)
            print("hdfs dir path is -> ", dir_path)
            # fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            #
            # if not fs.exists(sc._jvm.org.apache.hadoop.fs.Path(dir_path)):
            #     print("invalid redis list record, skipped")
            #     DB.rpop('count-kfb')
            #     continue
            if not path_exist(dir_path):
                # print("invalid redis list record, skipped")
                # DB.rpop('count-kfb')
                continue

            cnt_rdd = sc.textFile(dir_path)
            current_count = cnt_rdd.count()
            print("current cnt is -> ", current_count, "   total cnt is -> ", total_count)
            # if total_count == current_count:
            if current_count == total_count:
                # reduce to get the result
                btime = time.time()
                get_result(cnt_rdd)
                print("reduce time elapsed ", time.time() - btime)
                DB.lpop('count-kfb')
            else:
                print("write not ends")
                print(type(total_count), type(current_count))

        print("queue is empty")
        time.sleep(30)

