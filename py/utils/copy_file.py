from shutil import copyfile
import os
from multiprocessing import Process


def copy_thr_num_file(src, dst, thr_num):
    procs = []
    for i in range(thr_num):
        process = Process(target=copy_per_thr, args=(src, dst, i))
        procs.append(process)
        process.start()
    for proc in procs:
        proc.join()
    print(os.path.dirname(src))


def get_kfb_filename(src, dst, idx):
    root = os.path.dirname(src)
    fname = os.path.basename(src).split('.')
    assert len(fname) == 2
    pre, post = fname[0], fname[1]
    return os.path.join(dst, pre + "_" + str(idx) + "." + post)


def copy_per_thr(src, dst, idx):

    file_name = get_kfb_filename(src, dst, idx)
    kfb_dst = os.path.join(dst, file_name)
    copyfile(src, kfb_dst)


if __name__ == '__main__':
    copy_thr_num_file('/home/litchy/health/233.txt', 3)
