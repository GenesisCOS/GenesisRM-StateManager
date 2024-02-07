import time
import threading 
import datetime 
from logging import Logger 

import pandas as pd
import numpy as np
from mapie.subsample import BlockBootstrap
from mapie.regression import MapieTimeSeriesRegressor
from sklearn.svm import SVR

from ..util.postgresql import PostgresqlDriver


OUTPUT_DIR = 'autoscaler/logs/'


def series_to_supervised(data, n_in, n_out, dropna=True):
    # n_vars = 1 if type(data.py) is list else data.py.shape[1]
    df = pd.DataFrame(data)
    cols = list()
    # input sequence (t-n, ... t-1)
    for i in range(n_in, 0, -1):
        cols.append(df.shift(i))
    # forecast sequence (t, t+1, ... t+n)
    for i in range(0, n_out):
        cols.append(df.shift(-i))
    # put it all together
    agg = pd.concat(cols, axis=1)
    # drop rows with NaN values
    if dropna:
        agg.dropna(inplace=True)
    return agg.values


class EnbpiPredictor(object):

    def __init__(self, cfg, logger: Logger, 
                 service_name, endpoint_name: str, 
                 metrics: str,
                 agg_function='mean'):
        
        self.__logger = logger 
        
        self.raw_regressor = SVR()
        self.regressor = SVR()

        self.bootstrap = BlockBootstrap(
            n_resamplings=60, length=10, overlapping=True, random_state=59
        )

        self.sql_driver = PostgresqlDriver(
            host=cfg.base.postgresql.host,
            port=cfg.base.postgresql.port,
            user=cfg.base.postgresql.user,
            password=cfg.base.postgresql.password,
            database=cfg.base.postgresql.database
        )
        
        self.__service_name = service_name
        self.__endpoint_name = endpoint_name
        self.__metrics = metrics

        self.agg_function = agg_function

        self.history_len = cfg.ts_predictor.enbpi.history_len
        self.sample_x_len = cfg.ts_predictor.enbpi.sample_x_len
        self.sample_y_len = cfg.ts_predictor.enbpi.sample_y_len
        self.alpha = cfg.ts_predictor.enbpi.alpha
        
        self.__estimator: MapieTimeSeriesRegressor = None 
        self.__estimator_lock = threading.Lock()
        
        self.db_table = cfg.ts_predictor.enbpi.db.table_name 
        
        self.__last_predict_X = None 
        self.__last_predict_ts = None 
        
    def __fit_new_model(self):
        estimator = MapieTimeSeriesRegressor(
                SVR(), 
                method="enbpi", 
                cv=self.bootstrap, 
                agg_function=self.agg_function, 
                n_jobs=-1
            ) 
        
        # 0. Fetch data from postgresql 
        data = self.fetch_data(limit=self.history_len)
        if len(data) < self.history_len:
            return None 
        
        # 1. Transform a time series dataset into 
        #    a supervised learning dataset
        fit_values = data['value'].values
        fit_values.reshape(len(fit_values), 1)
        fit_series = series_to_supervised(fit_values, self.sample_x_len, self.sample_y_len)
        fit_x, fit_y = fit_series[:, :-self.sample_y_len], fit_series[:, -self.sample_y_len:]
        
        # 1.1. Aggregate using max 
        fit_y_ = list()
        for i in fit_y:
            fit_y_.append(np.max(i))
        fit_y_ = np.array(fit_y_)
        fit_y_.reshape(len(fit_y_), -1)
        
        # 2. Fit
        estimator = estimator.fit(fit_x, fit_y_)
        
        return estimator 
    
    def init(self):
        self.__estimator = self.__fit_new_model()
        
    def refit(self):
        def __refit():
            estimator = self.__fit_new_model()
            self.__estimator_lock.acquire()
            self.__estimator = estimator
            self.__estimator_lock.release()
        threading.Thread(target=__refit, daemon=True).start()
        
    def fetch_data(self, limit) -> pd.DataFrame:
        sql = f"SELECT " \
              f"    {self.__metrics} AS value, " \
              f"    time AS time, " \
              f"    timestamp AS timestamp "  \
              f"FROM " \
              f"    {self.db_table} " \
              f"WHERE " \
              f"    service_name = '{self.__service_name}' AND " \
              f"    endpoint_name = '{self.__endpoint_name}' AND " \
              f"    window_size = 1000 AND " \
              f"    timestamp > {int(time.time() * 1000) - (self.history_len * 2) * 1000} "  \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT " \
              f"    {limit};"

        data = self.sql_driver.exec(sql)
        data.sort_values('time', inplace=True)
        return data 

    def predict(self):
        self.__estimator_lock.acquire()
        
        if self.__estimator is None:
            self.__estimator_lock.release()
            return dict(
                result=dict(
                    upper_bound=0,
                    lower_bound=0
                ),
                time_stats=dict(
                    sql_use_time=0
                ) 
            )
        
        # Fetch data from postgresql
        sql_start_time = time.time()
        data = self.fetch_data(limit=self.sample_x_len)
        sql_use_time = time.time() - sql_start_time
        
        # Update 
        if self.__last_predict_X is not None and \
                self.__last_predict_ts in data.timestamp.values:
                    
            __data = data[data.timestamp <= self.__last_predict_ts]
            __data = data[data.timestamp >= self.__last_predict_ts - 1000 * (self.sample_y_len - 1)]
            y = np.max(__data.value.values)
            y = np.array([[y]])
            
            self.__estimator.update(
                X=self.__last_predict_X,
                y=y
            )
            
        elif self.__last_predict_X is not None and \
                self.__last_predict_ts < data.timestamp.values[0]:
            self.__logger.info('Miss update.')

        x = data.value.values.reshape(1, -1)
        self.__last_predict_X = x 
        self.__last_predict_ts = data.timestamp.values[-1] + \
            1000 * self.sample_y_len

        # Predict
        pred_y_pfit = np.zeros(1)
        pis_y_pfit = np.zeros((1, 2, 1))

        pred_y_pfit[:1], pis_y_pfit[:1, :, :] = self.__estimator.predict(
            X=x, 
            alpha=self.alpha, 
            ensemble=True, 
            optimize_beta=True
        )
        
        self.__estimator_lock.release()

        upper_bound = pis_y_pfit[0][1][0]
        lower_bound = pis_y_pfit[0][0][0]

        return dict(
            result=dict(
                upper_bound=upper_bound,
                lower_bound=lower_bound
            ),
            time_stats=dict(
                sql_use_time=sql_use_time
            ) 
        )



