import time
import os
import pandas as pd
import numpy as np
import pickle

from mapie.subsample import BlockBootstrap
from mapie.regression import MapieTimeSeriesRegressor
from statsmodels.tsa._stl import STL
from util.postgresql import PostgresqlDriver

# 参数 a 是显著性水平，当置信度 99% 时， a 应该为 0.01
def stl_ana(resid, a):
    lenth = len(resid)
    k1 = int(lenth * a / 2)
    k2 = int(lenth * (1 - a / 2))
    df_resid_sorted = sorted(resid)
    train_lower = df_resid_sorted[k1]
    train_higher = df_resid_sorted[k2]
    # print(train_lower, train_higher)
    return train_lower, train_higher

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
        
        self.prev_upper_shift = 15.51
        self.prev_lower_shift = -15.51

        self.cnt = 0
        self.stl_train = None
        with open("./autoscaler/sttf/stl_train copy.pkl", 'rb') as file:
            self.stl_train = pickle.load(file)



    def predict(self, service_name, endpoint_name, start_time):        
        # print(f'start_time: {start_time}')
        
        
        # print(f'diff:{time.time() - start_time}')
        # print(f'cnt:{self.cnt}')
        # print(f'time.time() - start_time) // 60{(time.time() - start_time) // 60}')
        # current_directory = os.getcwd()
        # print("当前工作目录：", current_directory)
        if (time.time() - start_time) // 60 == self.cnt:
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
            raw_estimator = self.raw_regressor.fit(fit_x, fit_y.reshape(len(fit_y), ))
            use_time = time.time() - t0
            self.logger.debug(f'fit done, use {use_time} seconds')

            x = fit_values[len(fit_values) - self.sample_x_len:]

            # Predict
            # pred_y_pfit[:1], pis_y_pfit[:1, :, :] = estimator.predict(
            #     x.reshape(1, len(x)), alpha=self.alpha, ensemble=True, optimize_beta=True
            # )
            predict_value = raw_estimator.predict(x.reshape(1, len(x)))[0]

            upper_bound = predict_value + self.prev_upper_shift
            lower_bound = predict_value + self.prev_lower_shift
            lower_bound = lower_bound if lower_bound > 0 else 0

            # print(f'over_pfit2{type(over_pfit),over_pfit}')
            # print(f'down_pfit2{type(over_pfit),down_pfit}')

            # predict_value = pred_y_pfit[0]

            self.logger.debug(f'last value: {fit_values[-1]}')
            self.logger.debug(f'predict value: {predict_value}')
            self.logger.debug(f'upper bound: {upper_bound}')
            self.logger.debug(f'lower bound: {lower_bound}')
            # print(f'upper bound1: {self.prev_upper_shift}')

            return predict_value, upper_bound, lower_bound, fit_values[-1], use_time 

        self.cnt += 1
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
        raw_estimator = self.raw_regressor.fit(fit_x, fit_y.reshape(len(fit_y), ))
        use_time = time.time() - t0
        self.logger.debug(f'fit done, use {use_time} seconds')

        x = fit_values[len(fit_values) - self.sample_x_len:]

        # Predict
        # pred_y_pfit[:1], pis_y_pfit[:1, :, :] = estimator.predict(
        #     x.reshape(1, len(x)), alpha=self.alpha, ensemble=True, optimize_beta=True
        # )
        predict_value = raw_estimator.predict(x.reshape(1, len(x)))[0]

        T = 960
        # 以下重新拟合 STL
        # with open("./autoscaler/sttf/stl_train.pkl", 'rb') as file:
        #     stl_train = pickle.load(file)
        self.stl_train = np.append(self.stl_train[1:], np.average(fit_values[-12:]))
        with open("./autoscaler/sttf/stl_train_2.pkl", 'wb') as file:
            pickle.dump(self.stl_train, file)

        res1 = STL(self.stl_train, period=T).fit()
        res2 = STL(res1.seasonal, period=T).fit()
        l1, u1 = stl_ana(res1.resid, self.alpha)
        l2, u2 = stl_ana(res2.resid, self.alpha)

        self.prev_upper_shift = u1 + u2
        self.prev_lower_shift = l1 + l2
        upper_bound = predict_value + self.prev_upper_shift
        lower_bound = predict_value + self.prev_lower_shift 
        lower_bound = lower_bound if lower_bound > 0 else 0

        # print(f'over_pfit2{type(over_pfit),over_pfit}')
        # print(f'down_pfit2{type(over_pfit),down_pfit}')

        # predict_value = pred_y_pfit[0]
        
        self.logger.debug(f'last value: {fit_values[-1]}')
        self.logger.debug(f'predict value: {predict_value}')
        self.logger.debug(f'upper bound: {upper_bound}')
        self.logger.debug(f'lower bound: {lower_bound}')
        # print(f'upper bound2: {self.prev_upper_shift}')

        return predict_value, upper_bound, lower_bound, fit_values[-1], use_time 