import asyncio
import aiohttp
import functools
import logging
import queue
from threading     import Thread
from misc          import catch_errors
from record_worker import RecordWorker

# Version handling
if not hasattr(asyncio, 'ensure_future'):
    asyncio.ensure_future = asyncio.async


class AsyncScraper(object):
    def __init__(self, conn_factory, logger, staging_tbl, proxy=None):
        logger.info('Initializing AsyncScraper')
        self._conn_factory = conn_factory
        self._logger       = logger
        self._staging_tbl  = staging_tbl
        self._proxy        = proxy

        if hasattr(asyncio.Queue, 'join'):
            jobsq = asyncio.Queue()
        else: 
            jobsq = asyncio.JoinableQueue()
        self._jobsq = jobsq
        
        errors   = {}
        
        # Set up the queue and the listener to stage the records
        logger.debug('Initializing Queue')
        self._db_queue = queue.Queue()
        Worker         = RecordWorker(self._db_queue, conn_factory, 
                                      models.staging_table, models.log_table,
                                      True)
        work_thread = Thread(target=Worker.record_inserter)
        self._dbworker = Worker


        work_thread.start()
        logger.debug('Queue initialized')

    @asyncio.coroutine
    def parse_data(self, data, *args, **kwargs):
        """Takes whatever data is returned by the get_data() method and parses 
        it, returning the scraped data.  New jobs can be added to the work
        queue with a call to self.add_job().

        This method will be passed the raw data returned from get_data() as well
        as the arguments with which get_data was called."""
        raise NotImplementedError

    @asyncio.coroutine
    def get_data(self):
        """Helper method to translate the data needed to make an HTTP call
        into an actual HTTP call (or wherever data is coming from).  For 
        example, an implementation might be:

        def get_page(self, item_id):
            url = 'https://www.store.com/items'
            return (yield from self.post(url, data={'item_id':item_id}))

        More complex implementations may include error handling"""
        raise NotImplementedError

    def add_job(self, *args, **kwargs):
        """Adds a new job to the work queue"""
        new_job = self._prepare_job(*args, **kwargs)
        self._jobsq.put_nowait(new_job)

    @asyncio.coroutine
    def get(self, url, timeout=60, *args, **kwargs):
        """HTTP GET url with timeout"""
        req = aiohttp.request('get', url, *args, 
                              connector=self._proxy, **kwargs)
        return (yield from asyncio.wait_for(req, timeout))
    
    @asyncio.coroutine
    def post(self, url, timeout=60, *args, **kwargs):
        """HTTP POST url with timeout"""
        req = aiohttp.request('post', url, *args, 
                              connector=self._proxy, **kwargs)
        return (yield from asyncio.wait_for(req, timeout))

    @asyncio.coroutine
    def _run_queue(self):
        """Pull waiting jobs off of the queue and schedule their execution."""
        while True:
            coro = yield from self._jobsq.get()
            f = asyncio.ensure_future(coro)
            f.add_done_callback(lambda _: self._jobsq.task_done())

    def _prepare_job(self, *args, **kwargs):
        """Assemble the various parts into a job.  Chains together get_data() 
        with parse_data(), the result of which is, in turn, sent to the db 
        queue."""
        self._logger.debug('New job being created', 
                    extra={'job_info':(args, kwargs)})

        @catch_errors(self._logger)
        @asyncio.coroutine
        def data_job(self, *args, **kwargs):
            with (yield from self._sem):
                raw_data = yield from self.get_data(*args, **kwargs)
            parser = self.parse_data(raw_data, *args, **kwargs)
            yield from self._send_data(parser)
        return data_job(self, *args, **kwargs)

    @asyncio.coroutine
    def _send_data(self, coro):
        """Run coro, sending the returned data to the processing queue"""
        data = yield from coro
        if data:
            logger.debug('Sending results to queue')
            try:
                db_queue.put_nowait(data)
            except Exception as e:
                logger.info('Error enqueuing results', exc_info=True, 
                            extra={'new_data':data})

    def scrape(self, max_async=100):
        """Run the scraper.  Set max_async to control the maximum 
        number of concurrent tasks."""
        self._sem = asyncio.Semaphore(max_async)
        loop = asyncio.get_event_loop()

        asyncio.ensure_future(self._run_queue(self._jobsq))
        loop.run_until_complete(asyncio.ensure_future(self._jobsq.join()))

        # Send a signal to the worker and wait for the work queue to empty
        self._dbworker.terminate()
        self._db_queue.join()
