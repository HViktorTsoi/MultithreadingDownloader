"""
多线程下载器
By HViktorTsoi

"""
import sys
import threading
import time
from contextlib import closing

import requests
import requests.adapters


class Downloader:
    """多线程下载器

    用Downloader(目标URL, 保存的本地文件名)的方式实例化

    用start()开始下载

    样例:

    downloader=Downloader(
        url="http://file.example.com/somedir/filename.ext",
        file="/path/to/file.ext"
    )

    downloader.start()
    """

    def __init__(self, threads_num=20, chunk_size=1024 * 128, timeout=60):
        """初始化

            :param threads_num=20: 下载线程数

            :param chunk_size=1024*128: 下载线程以流方式请求文件时的chunk大小

            :param timeout=60: 下载线程最多等待的时间
        """
        self.threads_num = threads_num
        self.chunk_size = chunk_size
        self.timeout = timeout

        self.__content_size = 0
        self.__file_lock = threading.Lock()
        self.__downloaded_size = 0
        self.__threads_status = {}

        requests.adapters.DEFAULT_RETRIES = 2

    def __establish_connect(self, url):
        """建立连接
        """
        print("建立连接中......")
        hdr = requests.head(url).headers
        self.__content_size = int(hdr["Content-Length"])
        self.__downloaded_size = 0
        print("连接已经建立.\n文件大小：{}B".format(self.__content_size))

    def __page_dispatcher(self):
        """分配每个线程下载的页面大小
        """
        # 基础分页大小
        basic_page_size = self.__content_size // self.threads_num
        pages = []
        start_pos = 0
        # 分配给各线程的页面大小
        while start_pos + basic_page_size < self.__content_size:
            pages.append({
                'start_pos': start_pos,
                'end_pos': start_pos + basic_page_size
            })
            start_pos += basic_page_size + 1
        # 最后不完整的一页补齐
        pages.append({
            'start_pos': start_pos,
            'end_pos': self.__content_size - 1
        })
        return pages

    def __download(self, url, file, page):
        """下载

            :param url="": 下载的目标URL

            :param file: 保存在本地的目标文件

            :param page: 下载的分页信息 start_pos:开始字节 end_pos:结束字节
        """
        # 当前线程负责下载的字节范围
        headers = {
            "Range": "bytes={}-{}".format(page["start_pos"], page["end_pos"])
        }
        thread_name = threading.current_thread().name
        # 初始化进程信息列表
        self.__threads_status[thread_name] = {
            "page_size": page["end_pos"] - page["start_pos"],
            "page": page,
            "status": 0,
        }
        try:
            # 以流的方式进行get请求
            with closing(requests.get(
                url=url,
                headers=headers,
                stream=True,
                timeout=self.timeout
            )) as response:
                for data in response.iter_content(chunk_size=self.chunk_size):
                    # 向目标文件中写入数据块,此处需要同步锁
                    with self.__file_lock:
                        # 查找文件写入位置
                        file.seek(page["start_pos"])
                        # 写入文件
                        file.write(data)
                        self.__downloaded_size += len(data)
                    # 数据流每向前流动一次,将文件指针同时前移
                    page["start_pos"] += len(data)
                    self.__threads_status[thread_name]["page"] = page
                    self.__log_status()
        except Exception as e:
            print(e, file=sys.stderr)
            self.__threads_status[thread_name]["status"] = 1

    def __log_status(self):
        print("\033c")
        print("文件元信息:\nURL: {}\n文件名:{}".format(
            self.__threads_status["url"],
            self.__threads_status["target_file"]
        ))
        print("下载中......")
        for thread_name, thread_status in self.__threads_status.items():
            if thread_name not in ("url", "target_file"):
                page_size = thread_status["page_size"]
                page = thread_status["page"]
                status = thread_status["status"]
                if status == 0:
                    if page["start_pos"] < page["end_pos"]:
                        print("|- {}  Downloaded: {}KB / Chunk: {}KB".format(
                            thread_name,
                            (page_size - (page["end_pos"] -
                                          page["start_pos"])) / 1024,
                            page_size / 1024,
                        ))
                    else:
                        print("|=> {} Finished.".format(thread_name))
                elif status == 1:
                    print("|XXX {} Crushed.".format(thread_name))
        print("Downloaded: {}KB / Total: {}KB".format(
            self.__downloaded_size / 1024,
            self.__content_size / 1024
        ))

    def start(self, url, target_file, urlhandler=lambda u: u):
        """开始下载

            :param url="": 下载的目标URL

            :param urlhandler=lambdau:u: 目标URL的处理器，用来处理重定向或Content-Type不存在等问题

            :param file="": 保存在本地的目标文件名（完整路径，包括文件扩展名）
        """
        thread_list = []
        # 记录下载开始时间
        start_time = time.time()
        self.__establish_connect(url)
        self.__threads_status["url"] = url
        self.__threads_status["target_file"] = target_file
        with open(target_file, "wb+") as file:
            for page in self.__page_dispatcher():
                thd = threading.Thread(
                    target=self.__download, args=(urlhandler(url), file, page)
                )
                thd.start()
                thread_list.append(thd)
            for thd in thread_list:
                thd.join()
        # 记录下载总计用时
        span = time.time() - start_time
        self.__threads_status = {}
        print("总计用时:{}s".format(span))


if __name__ == '__main__':
    downloader = Downloader(
        threads_num=20
    )
    downloader.start(
        url="http://fjyd.sc.chinaz.com/Files/DownLoad/pic9/201709/bpic3616.rar",
        target_file="/tmp/tmp.rar",
    )
