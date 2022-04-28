

import hashlib
import multiprocessing
import os
from multiprocessing import Semaphore
from time import time

import multichecksum.monkey_patching as monkey_patching  # noqa

from .exceptions import ChecksumFailedException, PathNotFoundException

"""
1. OOP -> check
2. Error handling -> check
3. 'check_dir' return a patched json encoded object -> check
4. Implement it via multiprocessing and recursive(asynchronous and synchronous) -> check  # noqa
5. index indicate order -> check
6. PEP-8 -> check
"""


class CheckerResultObj:
    def __init__(
            self,
            directory: str,
            files_count: int,
            runtime: float,
            concurrency: int,
            checksums_objects: list
    ) -> None:
        self.directory = directory
        self.files_count = files_count
        self.runtime = runtime
        self.concurrency = concurrency
        self.checksums_objects = checksums_objects

    def __iter__(self):
        """
        Override __iter__ magic function to enable
        json encoder to map it correctly
        """
        for attr in self.__dict__.items():
            if isinstance(attr, list):
                for i in attr:
                    yield i
            else:
                yield attr

    def to_json(self):
        """
        patched json encoder default function
        """
        return {
            'metadata': {
                'directory': self.directory,
                'files': self.files_count,
                'runtime': self.runtime,
                'concurrency': self.concurrency,
            },
            'checksums': self.checksums_objects
        }


class Checker:

    def __init__(self) -> None:
        self.concurrency = multiprocessing.cpu_count()
        self.checksums_objects = []

    def _map_dirs_and_files(
            self,
            dir_name: str,
            traversed: list = [],
            results: list = [],
            index: int = 1
    ):
        """
        Synchronous mapper

        :param dir_name: current path
        :param traversed: list to track directories
        :param results: list to store results
        :param index: index counter
        :return: checker result object
        """
        _, dirs, files = next(os.walk(dir_name))
        for f in dirs:
            sub_dir = os.path.join(dir_name, f)
            if os.path.isdir(sub_dir) and sub_dir not in traversed:
                traversed.append(sub_dir)
                index = self._map_dirs_and_files(
                    sub_dir, traversed, results, index)

        for f in files:
            sub_dir = os.path.join(dir_name, f)
            if os.path.isfile(sub_dir):
                results.append({
                    'file': sub_dir,
                    'index': index,
                    'checksum': self._checksum_file(sub_dir)
                })
                index += 1

        return index

    def check_dir(self, directory: str) -> CheckerResultObj:
        """
        Synchronous method of check_dir
        finds all sub-directories
        and index their files from deepest to most upper level using recursion.

        :param directory: folder root path
        :return: checker result object
        """
        if not os.path.isdir(directory):
            raise PathNotFoundException()

        ts = time()
        count = self._map_dirs_and_files(
            directory, [], self.checksums_objects) - 1
        te = time()
        return CheckerResultObj(
            directory=directory,
            files_count=count,
            runtime=te-ts,
            concurrency=self.concurrency,
            checksums_objects=self.checksums_objects
        )

    @staticmethod
    def _tic(cnt):
        with cnt.get_lock():
            cnt.value += 1

    def _mapper(self, cur_path, q, counter, sema):
        """
        Asynchronous mapper

        :param cur_path: current path
        :param q: shared queue
        :param counter: shared counter
        :param sema: semaphore for concurrency track
        :return: checker result object
        """
        print(f'HI IM: {multiprocessing.current_process()}')
        _, dirs, files = next(os.walk(cur_path))
        for f in dirs:
            sub_dir = os.path.join(cur_path, f)
            if os.path.isdir(sub_dir):
                sema.acquire()
                p = multiprocessing.Process(
                    target=self._mapper, args=(sub_dir, q, counter, sema))
                p.start()
                p.join()
                sema.release()

        for f in files:
            sub_dir = os.path.join(cur_path, f)
            if os.path.isfile(sub_dir):
                self._tic(counter)
                q.put({
                    'file': sub_dir,
                    'index': counter.value,
                    'checksum': self._checksum_file(sub_dir)
                })

        return True

    def check_dir_multi(self, directory: str) -> CheckerResultObj:
        """
        Asynchronous method of check_dir
        finds all sub directories
        and index their files from deepest to most upper level
        using multiprossing.

        :param directory: folder root path
        :return: checker result object
        """
        if not os.path.isdir(directory):
            raise PathNotFoundException()
        ts = time()
        q = multiprocessing.Queue()
        cnt = multiprocessing.Value('i', 0)
        sema = Semaphore(self.concurrency)
        p = multiprocessing.Process(
            target=self._mapper, args=(directory, q, cnt, sema))
        p.start()
        p.join()
        te = time()
        results = []
        while not q.empty():
            results.append(q.get())

        return CheckerResultObj(
            directory=directory,
            files_count=len(results),
            runtime=te-ts,
            concurrency=self.concurrency,
            checksums_objects=results
        )

    def _checksum_file(self, file_path: str) -> str:
        """
        open a file and generate md5 hash.

        :param file_path: file path
        :return: md5 hashed string
        """
        try:
            with open(file_path, 'rb') as file_to_check:
                data = file_to_check.read()
                return hashlib.md5(data).hexdigest()
        except ChecksumFailedException as e:
            print(f"Could not read file {file_to_check}, {e}")
        return ''
