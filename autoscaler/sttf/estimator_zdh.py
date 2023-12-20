import time

import pandas as pd
import numpy as np
from mapie.subsample import BlockBootstrap
from mapie.regression import MapieTimeSeriesRegressor

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
    r"""
    Short term time-series forecasting estimator

    Parameters
    ----------
    args:
        System arguments
    agg_function: str
        enbpi aggregate function, default is `mean`
    """

    def __init__(self, args, logger, agg_function='mean'):

        if args.sttf_regressor == 'linear':
            from sklearn.linear_model import LinearRegression
            self.regressor = LinearRegression()
        elif args.sttf_regressor == 'svr':
            from sklearn.svm import SVR
            self.raw_regressor = SVR()
            self.regressor = SVR()
        else:
            raise Exception(f'Not support regressor {args.sttf_regressor}')

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

        self.logger.debug(f'sttf alpha: {self.alpha}')

    def predict(self, service_name, endpoint_name):
        self.logger.debug(f'sttf predict max active task number of {service_name}:{endpoint_name}')
        sql = f"SELECT ac_task_max,time  FROM service_time_stats WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and (cluster = 'production' or cluster = 'cluster') " \
              f"and window_size = 5000 " \
              f"ORDER BY _time DESC " \
              f"LIMIT {self.history_len};"

        # Fetch data from postgresql
        data = self.sql_driver.exec(sql)
        data.sort_values('time', inplace=True)

        # Transform a time series dataset into a supervised learning dataset
        t0 = time.time()
        fit_values = data.ac_task_max.values
        fit_values.reshape(len(fit_values), 1)
        fit_series = series_to_supervised(fit_values, self.sample_x_len, self.sample_y_len)
        fit_x, fit_y = fit_series[:, :-self.sample_y_len], fit_series[:, -self.sample_y_len:]
        self.logger.debug(f'sample size: {len(fit_y)}')

        # Fit
        estimator = MapieTimeSeriesRegressor(
            self.regressor, method="enbpi", cv=self.bootstrap, agg_function=self.agg_function, n_jobs=-1
        )

        estimator = estimator.fit(
            fit_x, fit_y.reshape(len(fit_y), )
        )
        raw_estimator = self.raw_regressor.fit(fit_x, fit_y.reshape(len(fit_y), ))
        use_time = time.time() - t0
        self.logger.debug(f'fit done, use {use_time} seconds')

        x = fit_values[len(fit_values) - self.sample_x_len:]

        pred_y_pfit = np.zeros(1)
        pis_y_pfit = np.zeros((1, 2, 1))

        # Predict
        pred_y_pfit[:1], pis_y_pfit[:1, :, :] = estimator.predict(
            x.reshape(1, len(x)), alpha=self.alpha, ensemble=True, optimize_beta=True
        )
        predict_value = raw_estimator.predict(x.reshape(1, len(x)))[0]

        # predict_value = pred_y_pfit[0]
        upper_bound = pis_y_pfit[0][1][0]
        lower_bound = pis_y_pfit[0][0][0]

        self.logger.debug(f'last value: {fit_values[-1]}')
        self.logger.debug(f'predict value: {predict_value}')
        self.logger.debug(f'upper bound: {upper_bound}')
        self.logger.debug(f'lower bound: {lower_bound}')

        return predict_value, upper_bound, lower_bound, fit_values[-1], use_time 



