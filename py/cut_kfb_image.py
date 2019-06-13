import os
from kfbReader import reader
import DBParser
import cv2
import argparse
import sys
import time

from utils import streaming_image_producer

from multiprocessing import cpu_count
from multiprocessing import Process, Lock
num_thr = 3
# try:
#     num_thr = os.environ["OMP_NUM_THREADS"]
# except:
#     num_thr = cpu_count()

image_size = piece_size = 320
scale = 20

lock = Lock()


overlap = image_size / 2
class_map = "Background,0,ASC-H,1,ASC-US,2,LSIL,3,HSIL,4,AGC-NOS,5,AGC,6,AIS,7,AC,8,SCC,9,Germ,10,Candida,11,Trichomonad,12,Herpes,13,Actinomyces,14,Unidentified,0,Streptococcus,0"
dicts = class_map.split(',')
mapper = {}
for k, val in zip(dicts[::2], dicts[1::2]):
    mapper[k] = int(val)


def GetRoiInfo(path, scale=20):
    roi_info = []
    db_path = path.replace('.kfb', '.db')
    if os.path.exists(db_path):
        db = DBParser.ReadDB(db_path, 2)
        for d in db:
            if mapper.get(d['label']) is None:
                print('wrong type detect. ', d['label'], db_path)
                continue
            if mapper.get(d['label']) == 0 or mapper.get(d['label']) > 4:
                continue

            x = min(d['xmax'], d['xmin']) * scale
            y = min(d['ymax'], d['ymin']) * scale
            h = abs(d['ymax'] - d['ymin']) * scale
            w = abs(d['xmax'] - d['xmin']) * scale

            if w <= 20 or h <= 20:
                print('wrong db record:', d)
                continue

            roi_info.append([x, y, w, h])

        #print (roi_info)
    else:
        assert ('pos' not in path), 'the kfb is NOT negative slice but db file doesn\'t exist! do you forget to copy it?'
    return roi_info


def GetLabel(roi, x, y):
    for i in range(len(roi)):
        m = min(roi[i][0] + roi[i][2], x + image_size)
        n = min(roi[i][1] + roi[i][3], y + image_size)
        u = max(roi[i][0], x)
        v = max(roi[i][1], y)

        if m > u and n > v:
            return 1
    return 0


# def GenerateTestSamples(src, dest, flag, lock, scale = 20):
#     r = reader()
#     r.ReadInfo(src)
#
#     w = r.getWidth()
#     h = r.getHeight()
#
#     dest_dir = os.path.abspath(os.path.join(dest, os.path.splitext(os.path.basename(src))[0]))
#     if not os.path.exists(dest_dir):
#         os.makedirs(dest_dir)
#
#     roi_info = GetRoiInfo(src)
#
#     x = y = 0
#     while x < w and y < h:
#         fname = "{}/{}_{}.jpg".format(dest_dir, y, x)
#         if not flag:
#             img = r.ReadRoi(x, y, image_size, image_size, scale)
#             #print (fname, img.shape)
#             cv2.imwrite(fname, img)
#         label = GetLabel(roi_info, x, y)
#         x += overlap
#         if (x + image_size) >= w:
#             x = 0
#             y += overlap
#         lock.acquire()
#         try:
#             with open('test_img_label.txt', 'a+') as f :
#                 f.write(fname + ' ' + str(label) + '\n')
#         finally:
#             lock.release()

def parse_args():
    """
    Parse input arguments
    """
    parser = argparse.ArgumentParser(description='kfb test image generator: '+ str(image_size)+'x'+ str(image_size) + ' with stride ' + str(overlap))
    parser.add_argument('--src', dest='kfb_test_path',
                        help='the src folder path to the test kfb files',
                        default=None, type=str)
    parser.add_argument('--dst', dest='kfb_test_img_path',
                        help='the dest folder path to the generated images from kfb test folder',
                        default=None, type=str)
    parser.add_argument('--only_label', dest='only_label', help='optinal: the bool flag to control generating label only or img+label, default is False', default=False, type=lambda x: (str(x).lower() == 'true'))

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)

    args = parser.parse_args()

    if not os.path.exists(args.kfb_test_path):
        parser.print_help()
        sys.exit(1) 

    return args


def cut_one_image(src, dst, reader, start_x, start_y, w, h):

    print("width and height --> ", w, h)

    if w * h > 200000000:
        print("splited img")
        split_x = h // 2
        cut_one_image(src, dst, reader, start_x, start_y, w, split_x)
        cut_one_image(src, dst, reader, start_x + split_x, start_y, w, split_x)

    else:
        col_per_thr = w // num_thr
        s = time.time()
        origin_img = reader.ReadRoi(start_x, start_y, w, h, scale)

        roi_info = GetRoiInfo(src)
        print("read time elapse is ", time.time() - s)
        for thr_idx in range(num_thr):
            y = thr_idx * col_per_thr
            img = origin_img[:, y:y + col_per_thr]

            # process = Process(target=cut_per_thr, args=(img, col_per_thr, roi_info, dst, args.only_label, lock))
            process = Process(target=cut_per_thr, args=(img, dst, start_x, y))

            procs.append(process)
            process.start()
            print('start', thr_idx)

        for process in procs:
            process.join()
            # for x in range(h // num_thr):
        print("one cut elapse is ---> ", time.time() - s)
    return


# def cut_per_thr(img, col_per_thr, roi_info, output, flag, lock):
def cut_per_thr(img, output, start_x, start_y):

    print("region shape is --> ", img.shape)

    x = y = 0
    while y + piece_size < img.shape[1]:
        while x + piece_size < img.shape[0]:
            region = img[x:x + piece_size, y:y + piece_size]

            # if not flag:
                # print("output x y is ---> ", output, x, y)

            fname = "{}/{}_{}".format(output, x + start_x, y + start_y)


            # cv2.imwrite(fname, region)

            #label = GetLabel(roi_info, x, y)

            region = cv2.imencode(".jpg", region)[1]

            streaming_image_producer.image_enqueue(fname, region)

            # global image_count
            # image_count += 1

            # lock.acquire()
            # try:
            #     with open('test_img_label.txt', 'a+') as f :
            #         f.write(fname + ' ' + str(label) + '\n')
            # finally:
            #     lock.release()
            # lock.acquire()
            # with open('/tmp/233.txt', 'a+') as f:
            #     f.write(fname + '\n')
            # lock.release()

            x = x + piece_size
        y = y + piece_size
        x = 0
        # import time
        # time.sleep(3)
#kfb_test_path = '/home/ftian/dl_solutions-kfb/data/test'
#kfb_test_img_path = '/home/ftian/dl_solutions-kfb/data/test_img_overlap_image_size'


def get_kfb_pieces(w, h):
    piece = 1
    while w * h > 200000000:
        h //= 2
        piece *= 2
    return piece


if __name__ == '__main__':
    args = parse_args()
    src_dst = []
    for root, _, files in os.walk(args.kfb_test_path):
        for file in files:
            if file.endswith(".kfb"):
                src_dst.append([os.path.abspath(os.path.join(root, file)), os.path.abspath(args.kfb_test_img_path)])

    procs = []
    # lock = Lock()
    
    # for idx in range(len(src_dst)):
    #     data = src_dst[idx * set_num:(idx + 1) * set_num]
    #     print("len of data is --> ", len(data))

    print ("len is --> ", len(src_dst))
    print(src_dst)

    # for num in range(len(src_dst)):
    #     dest_dir = os.path.abspath(os.path.join(src_dst[num][1], os.path.splitext(os.path.basename(src_dst[num][0]))[0]))
    #     print (dest_dir)
    #     if not os.path.exists(dest_dir):
    #         print(dest_dir, "  not exist")
    #         os.makedirs(dest_dir)

    # time.sleep(60)
    for num in range(len(src_dst)):

        src = src_dst[num][0]
        dst_root = src_dst[num][1]

        dst = os.path.splitext(os.path.basename(src))[0]
        # dst = os.path.abspath(os.path.join(dst_root, os.path.splitext(os.path.basename(src))[0]))
        # if not os.path.exists(dst):
        #     os.makedirs(dst)
        # print("dst is --> ", dst)
        # time.sleep(60)
        r = reader()
        r.ReadInfo(src)
        w = r.getWidth()
        h = r.getHeight()

        # image_count = 0

        cut_one_image(src, dst, r, 0, 0, w, h)

        print("col number is ", w // image_size)
        print("row per thr is ", h // num_thr)
        print("row per thr is ", h // num_thr)

        pieces = get_kfb_pieces(w, h)
        image_count = (h // pieces // image_size) * pieces \
                      * (w // num_thr // image_size) * num_thr

        import redis
        from utils.helpers import settings
        DB = redis.StrictRedis(host=settings.REDIS_HOST,
                               port=settings.REDIS_PORT, db=settings.REDIS_DB)
        DB.lpush('count-kfb', (src + '|' + str(image_count)))
        print("image cnt is -->", image_count)
        time.sleep(3600)



