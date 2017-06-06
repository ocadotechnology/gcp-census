import logging
from itertools import islice
from google.appengine.api.taskqueue import TransientError, Queue
from google.appengine.runtime.apiproxy_errors import DeadlineExceededError
from gcp_census.decorators import retry


class Tasks(object):

    @classmethod
    def split_every(cls, n, iterable):
        i = iter(iterable)
        piece = list(islice(i, n))
        while piece:
            yield piece
            piece = list(islice(i, n))

    @classmethod
    def schedule(cls, queue_name, tasks):
        queue = Queue(queue_name)
        batch_size = 100
        task_count = 0
        for task_batch in cls.split_every(batch_size, tasks):
            cls._add_single_batch(queue, task_batch)
            task_count += len(task_batch)
        if task_count > 0:
            logging.info("Scheduled %d tasks in max %d batches",
                         task_count, batch_size)

    @classmethod
    @retry((DeadlineExceededError, TransientError), tries=6, delay=2, backoff=2)
    def _add_single_batch(cls, queue, task_batch):
        if task_batch:
            queue.add(task_batch)
            logging.info("Scheduled %d tasks", len(task_batch))
