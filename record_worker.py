import logging
import queue
import sqlalchemy as sa

#import models
from db_logger import QDBLogger


class RecordWorker(object):
    def __init__(self, q, conn_factory, tbl, log_tbl, debug_mode=True):
        self._terminated   = False
        self._queue        = q
        self._logger       = QDBLogger(conn_factory, log_tbl, 'ctrip.record_worker')
        self._debug_mode   = debug_mode
        self._conn_factory = conn_factory
        self._tbl          = tbl

        self._logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)
        self._logger.info('Initializing database worker')

        self._pool = self._get_db_connection()
        self._pool.execute(self._tbl.delete())
        self._logger.debug('Old data cleared') 
        self._records = []
        
    def _get_db_connection(self):
        self._logger.debug('Establishing worker DB connection')
        pool = self._conn_factory()
        pool.autocommit = True
        self._logger.debug('Worker DB connection established')
        return pool        
    
    def terminate(self):
        self._logger.info('Termination signal received.')
        self._terminated = True
        
    def _try_insert(self, max_tries=5):
        for i in range(max_tries):
            try:
                self._pool.execute(self._tbl.insert().values(self._records))
                return True
            except sa.exc.DBAPIError as e:
                if e.connection_invalidated:
                    self._logger.info('Connection invalidated.  Reconnecting.',
                                       exc_info=True)
                    del self._pool
                    self._pool = self._get_db_connection()   
                else:
                    self._logger.info('DBAPI exception raised inserting records.',
                                       exc_info=True)
            except Exception as e:
                self._logger.info('Other Exception raised inserting records.',
                                   exc_info=True)
        return False
        
    def record_inserter(self, batch_size=2000, timeout=60):
        """This is a worker thread that takes hotel records off the queue (q)
        and, when enough are staged, does a big insert"""
        
        logger     = self._logger
        debug_mode = self._debug_mode
        q          = self._queue
        
        new_data  = None
        while not self._terminated:
            if debug_mode:
                logger.debug('Waiting for new data...')
            try:
                new_data = q.get(block=True, timeout=timeout)
            except queue.Empty as e:
                logger.info('No data in queue')
                continue
            except Exception as e:
                logger.info('Exception raised getting data from queue', exc_info=True)

            if new_data:
                logger.debug('New data found') 
                self._records.extend(new_data)
                logger.debug('Current buffer length: %d' % len(self._records)) 
                if len(self._records) >= batch_size:
                    logger.debug('Inserting %d records' % len(self._records))
                    if self._try_insert():
                        self._records = []
                    else:
                        logger.warning('Failure inserting records')
                logger.debug('New records are in')
            q.task_done()            
        
        logger.debug('Loop exited. Inserting remaining records.')
        if self._records:
            logger.debug('Inserting %d remaining records' % len(self._records))
            if self._try_insert():
                logger.debug('Records successfully inserted')
            else:
                logger.warning('Failure inserting records')
        logger.info('Worker exiting')
