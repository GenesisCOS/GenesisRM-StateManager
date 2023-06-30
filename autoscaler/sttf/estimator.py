import time

import pandas as pd
import numpy as np
from mapie.subsample import BlockBootstrap
from mapie.time_series_regression import MapieTimeSeriesRegressor

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

    def __init__(self, args, agg_function='mean'):

        if args.sttf_regressor == 'linear':
            from sklearn.linear_model import LinearRegression
            self.regressor = LinearRegression()
        elif args.sttf_regressor == 'svr':
            from sklearn.svm import SVR
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

        self.agg_function = agg_function

        self.table = args.task_stats_table
        self.history_len = args.sttf_history_len
        self.sample_x_len = args.sttf_sample_x_len
        self.sample_y_len = args.sttf_sample_y_len
        self.alpha = args.sttf_alpha

        print(f'sttf alpha: {self.alpha}')

    def predict(self, service_name, endpoint_name):
        print('=' * 10 + 'sttf')
        print(f'sttf predict max active task number of {service_name}:{endpoint_name}')
        sql = f"SELECT ac_task_max,time  FROM {self.table} WHERE " \
              f"sn = '{service_name}' and en = '{endpoint_name}' and cluster = 'production' " \
              f"and window_size = 5000 " \
              f"ORDER BY _time DESC " \
              f"LIMIT {self.history_len};"

        # Fetch data from postgresql
        t0 = time.time()
        data = self.sql_driver.exec(sql)
        data.sort_values('time', inplace=True)
        print(f'sttf sql query use {time.time() - t0} seconds')

        # Transform a time series dataset into a supervised learning dataset
        fit_values = data.ac_task_max.values
        fit_values.reshape(len(fit_values), 1)
        fit_series = series_to_supervised(fit_values, self.sample_x_len, self.sample_y_len)
        fit_x, fit_y = fit_series[:, :-self.sample_y_len], fit_series[:, -self.sample_y_len:]
        print(f'sample size: {len(fit_y)}')

        # Fit
        print('fitting ...')
        estimator = MapieTimeSeriesRegressor(
            self.regressor, method="enbpi", cv=self.bootstrap, agg_function=self.agg_function, n_jobs=-1
        )

        t0 = time.time()
        estimator = estimator.fit(
            fit_x, fit_y.reshape(len(fit_y), )
        )
        print(f'fit done, use {time.time() - t0} seconds')

        x = fit_values[len(fit_values) - self.sample_x_len:]

        pred_y_pfit = np.zeros(1)
        pis_y_pfit = np.zeros((1, 2, 1))

        # Predict
        pred_y_pfit[:1], pis_y_pfit[:1, :, :] = estimator.predict(
            x.reshape(1, len(x)), alpha=self.alpha, ensemble=True, optimize_beta=True
        )

        predict_value = pred_y_pfit[0]
        upper_bound = pis_y_pfit[0][1][0]
        lower_bound = pis_y_pfit[0][0][0]

        print('last value: ', fit_values[-1])
        print(f'predict value: {predict_value}')
        print(f'upper bound: {upper_bound}')
        print(f'lower bound: {lower_bound}')

        print('=' * 10 + 'sttf done')

        return predict_value, upper_bound, lower_bound, fit_values[-1]



