import time

import pandas as pd
import numpy as np
from mapie.subsample import BlockBootstrap
from mapie.regression import MapieTimeSeriesRegressor
from sklearn.svm import SVR

from util.postgresql import PostgresqlDriver


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


class SVREstimator(object):
    def __init__(self) -> None:
        pass


class Estimator(object):

    def __init__(self, args, logger, agg_function='mean'):
            
        self.raw_regressor = SVR()
        self.regressor = SVR()

        self.bootstrap = BlockBootstrap(
            n_resamplings=60, length=10, overlapping=True, random_state=59
        )

        self.sql_driver = PostgresqlDriver(
            host=args.postgres_host,
            port=args.postgres_port,
            user=args.postgres_user,
            password=args.postgres_passwd,
            database=args.postgres_db
        )
        
        self.logger = logger 

        self.agg_function = agg_function

        self.history_len = args.sttf_history_len
        self.sample_x_len = args.sttf_sample_x_len
        self.sample_y_len = args.sttf_sample_y_len
        self.alpha = args.sttf_alpha

    def predict(self, service_name, endpoint_name):
        self.logger.debug(f'Predicting throughput of {service_name}:{endpoint_name}.')
        
        # 0. Fetch data from postgresql
        sql_start_time = time.time()
        sql = f"SELECT " \
              f"    (span_count / 5) AS throughput, " \
              f"    time AS time " \
              f"FROM " \
              f"    span_stats " \
              f"WHERE " \
              f"    service_name = '{service_name}' AND " \
              f"    endpoint_name = '{endpoint_name}' AND " \
              f"    window_size = 5000 " \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT " \
              f"    {self.history_len};"

        data = self.sql_driver.exec(sql)
        sql_use_time = time.time() - sql_start_time
        self.logger.debug(f'[TP-Prediction {service_name}:{endpoint_name}] Fetch data use {sql_use_time} seconds.')
        
        data.sort_values('time', inplace=True)

        # 1. Transform a time series dataset into 
        #    a supervised learning dataset
        fit_start_time = time.time()
        
        fit_values = data.ac_task_max.values
        fit_values.reshape(len(fit_values), 1)
        fit_series = series_to_supervised(fit_values, self.sample_x_len, self.sample_y_len)
        fit_x, fit_y = fit_series[:, :-self.sample_y_len], fit_series[:, -self.sample_y_len:]
        self.logger.debug(f'sample size: {len(fit_y)}')

        # 2. Fit
        estimator = MapieTimeSeriesRegressor(
            self.regressor, method="enbpi", cv=self.bootstrap, agg_function=self.agg_function, n_jobs=-1
        )

        estimator = estimator.fit(fit_x, fit_y.reshape(len(fit_y), ))
        raw_estimator = self.raw_regressor.fit(fit_x, fit_y.reshape(len(fit_y), ))
        
        fit_use_time = time.time() - fit_start_time
        self.logger.debug(f'[TP-Prediction {service_name}:{endpoint_name}]Fit done, use {fit_use_time} seconds.')

        x = fit_values[len(fit_values) - self.sample_x_len:]

        # 3. Predict
        pred_y_pfit = np.zeros(1)
        pis_y_pfit = np.zeros((1, 2, 1))

        pred_y_pfit[:1], pis_y_pfit[:1, :, :] = estimator.predict(
            x.reshape(1, len(x)), alpha=self.alpha, ensemble=True, optimize_beta=True
        )
        predict_value = raw_estimator.predict(x.reshape(1, len(x)))[0]

        # predict_value = pred_y_pfit[0]
        upper_bound = pis_y_pfit[0][1][0]
        lower_bound = pis_y_pfit[0][0][0]

        self.logger.debug(f'[TP-Prediction {service_name}:{endpoint_name}] Last value: {fit_values[-1]}.')
        self.logger.debug(f'[TP-Prediction {service_name}:{endpoint_name}] Predict value: {predict_value}.')
        self.logger.debug(f'[TP-Prediction {service_name}:{endpoint_name}] Upper bound: {upper_bound}.')
        self.logger.debug(f'[TP-Prediction {service_name}:{endpoint_name}] Lower bound: {lower_bound}.')

        return predict_value, upper_bound, lower_bound, fit_values[-1], dict(
            fit_use_time=fit_use_time,
            sql_use_time=sql_use_time
        ) 



