#coding=UTF-8

import random

url_paths = [
    "class/112.html",
    "class/128.html",
    "class/145.html",
    "class/146.html",
    "class/131.html",
    "class/130.html",
    "learn/821",
    "course/list"
]

ip_slices = [132,156,124,10,29,167,143,187,30,46,55,63,72,87,98,168]

def sample_url():
    return random.sample(url_paths, 1)[0]

def sample_ip():
    slice = random.sample(ip_slices, 4)
    return ".".join([str(item) for item in slice])

def generate_log(count = 10):
    while count >= 1:
        query_log = "{url}\t{ip}".format(url=sample_url(), ip=sample_ip())
        print query_log
        count = count - 1


if __name__ == '__main__':
    generate_log()
