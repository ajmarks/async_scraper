import json
import logging
import logging.handlers
import sqlalchemy as sa
import traceback
import queue
from datetime import datetime
from sqlalchemy.dialects import postgresql as pg_types

import models

class DBHandler(logging.Handler):
    def __init__(self, conn_factory, tbl, *args, **kwargs):
        #print('Initializing logger')
        super().__init__(*args, **kwargs)
        self._tbl               = tbl
        self._get_db_connection = conn_factory
        self._pool              = self._get_db_connection()
        self._pool.autocommit   = True
    
    def _try_insert(self, data, max_tries=5):
        for i in range(max_tries):
            try:
                self._pool.execute(self._tbl.insert().values(data))
                return True
            except sa.exc.DBAPIError as e:
                if e.connection_invalidated:
                    del self._pool
                    self._pool = self._get_db_connection()
                else:
                    self._logger.info('DBAPI exception raised inserting records.',
                                       exc_info=True)
            except Exception as e:
                self._logger.info('Other Exception raised inserting records.',
                                   exc_info=True)
        return False


    def emit(self, record):
        info = record.__dict__
        if info['exc_info']:
            info['exc_info'] = traceback.format_exception(*info['exc_info'])
        data = {'tstamp'       : datetime.fromtimestamp(record.created),
                'function'     : record.funcName,
                'level'        : record.levelno,
                'short_message': record.msg,
                'event_info'   : json.dumps(info)
               }
        self._try_insert(data)
                
    def __del__(self):
        self._pool.close()
        #super().__del__()

class QDBLogger(logging.Logger):
    def __init__(self, conn_factory, tbl, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        self.que = queue.Queue(-1)
        queue_handler = logging.handlers.QueueHandler(self.que)
        
        dbh = DBHandler(conn_factory, tbl)
        self.listener = logging.handlers.QueueListener(self.que, dbh)
        
        self.addHandler(queue_handler)
        self.addHandler(logging.StreamHandler())
        self.listener.start()
        
    def __del__(self):
        #self.que.join()
        self.listener.stop()
        #super().__del__()
