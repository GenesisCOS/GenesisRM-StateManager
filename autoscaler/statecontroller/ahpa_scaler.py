from logging import Logger
import time
import sys 
import copy 
import os
import pickle
from typing import List, Dict, Tuple 
from concurrent import futures
from logging import Logger
import pathlib
import math

import numpy as np
import pandas as pd
import cvxpy as cp
import statsmodels.api as sm
from scipy.stats import norm

from .. import Scaler
from ..util.postgresql import PostgresqlDriver


class AHPAScaler(Scaler):
    def __init__(self, cfg, logger: Logger):    
        super().__init__(cfg, logger)
        self.__logger = logger.getChild('Main')
        self.wait = cfg.scaler.ahpa_scaler.wait

        self.db_table = cfg.scaler.ahpa_scaler.db.table_name 
        
        self.__cfg = cfg
                
        self.lt_pred_results_path = './autoscaler/data/ahpa_data/lt_results.pkl'
        self.lt_pred_results = list()

        self.st_pred_results = list()

        self.cpu_usage_conf = dict()
        self.cpu_scale_rate = cfg.scaler.ahpa_scaler.rate.cpu_scale_rate
        
        self.st_fetch_minutes = cfg.scaler.ahpa_scaler.st_fetch_minutes
        
        self.namespace = cfg.scaler.ahpa_scaler.namespace
        self.st_history_len = cfg.scaler.ahpa_scaler.st_history_len
        
        self.scale_in_rate = cfg.scaler.ahpa_scaler.rate.scale_in_rate
        self.scale_in_freq = cfg.scaler.ahpa_scaler.freq.scale_in_freq
        self.scale_out_freq = cfg.scaler.ahpa_scaler.freq.scale_out_freq
        
        self.ctrl_freq = min(self.scale_in_freq, self.scale_out_freq)
        self.max_time_sclice = abs(self.scale_in_freq - self.scale_out_freq)

    def pre_start(self):
        self.__logger.info('AHPA Scaler without RobustSTL preStart ...')
        
        # if os.path.exists(self.lt_pred_results_path):
        #     with open(self.lt_pred_results_path, 'rb') as file:        
        #         self.lt_pred_results = pickle.load(file)                 
        
        # else:
        #     ret_futures = list()
        #     results = list()
                    
        #     directory = "/opt/projects/microkube-python-v2/tools/promtheus-downloader/default-data-aggregated-1705215956/cpu_usage"
        #     t0 = time.time()
        #     with futures.ThreadPoolExecutor() as executor:
        #         for service in self.get_all_services_from_cfg():
        #             service_rsplit = service.rsplit('-',1)[0]
        #             path = directory + "/" + service_rsplit + ".csv"
        #             service_history_data = pd.read_csv(path).iloc[:,1].to_numpy().flatten()
        #             long_term_pred = LongTermPred(self.__cfg, self.__logger, service_history_data)
        #             future = executor.submit(long_term_pred.predict, service)
        #             ret_futures.append(future)
            
        #         for future in ret_futures:
        #             results.append(future.result())
                
        #     self.lt_pred_results = results
        #     print(f'cost time: {time.time()-t0}')
        #     print(self.lt_pred_results)
        #     self.__logger.info('AHPA Scaler preStart finish')

        #     with open(self.lt_pred_results_path, 'wb') as file:
        #         pickle.dump(results, file)
                
        for service_name in self.get_all_services_from_cfg():
            resources_config = copy.deepcopy(self.get_resources_config_from_cfg(service_name))
            for config in resources_config:
                if 'requests' in config['resources']:                
                    if 'cpu' in config['resources']['requests']:           
                        self.cpu_usage_conf[service_name] = float(config['resources']['requests']['cpu'].rsplit('m')[0]) / 1000.0 * self.cpu_scale_rate

        print(self.cpu_usage_conf)
        
    def start(self):
        self.__logger.info('AHPA Scaler start ...')
        self.__service_endpoints_map: Dict[str, List[str]] = dict()
        
        # Init replicas 
        services = self.get_all_services_from_cfg()
        self.replicas = dict()
        for service in services:
            
            self.replicas[service] = dict(
                replicas=1
            )
            
            endpoints = self.get_service_endpoints(service)
            self.__service_endpoints_map[service] = endpoints
            
        self.__services: List[str] = services 

        # Sync replcas 
        self.sync_replicas()
        
        wait = self.wait
        self.__logger.info(f'Wait for {wait} seconds.')
        time.sleep(wait)  

        self.locust_start_time = time.time()
        # Adjust about 1min(considering the time scale)
        
        last_result = dict()
        time_sclices = dict()
        for service in self.__services:
            last_result[service] = 1
            time_sclices[service] = self.max_time_sclice
        
        while True:
            control_loop_start = time.time()
            
            # TODO predict
            st_controller_result = self.st_controller()
            # lt_controller_result = self.lt_controller()

            # TODO set self.replicas 
            
            result = dict()
            
            for service in self.__services:
                # lt_ret = lt_controller_result.get(service)
                st_ret = st_controller_result.get(service)

                max_conf = self.get_service_max_replicas_from_cfg(service)
                _max = max(st_ret, 1)
                _max = min(_max, max_conf)
                
                scale_info = '/'                                
                # result[service] = dict(replicas=_max)
                if last_result[service] > _max and time_sclices[service] > 0:
                    result[service] = dict(replicas=last_result[service])
                    time_sclices[service] -= self.ctrl_freq
                else:
                    # Scale in or out
                    if last_result[service] < _max:
                        _max += int(self.scale_in_rate * (_max - last_result[service]))
                        _max = min(_max, max_conf)
                        scale_info = 'IN'
                    elif last_result[service] > _max:
                        scale_info = 'OUT'
                    result[service] = dict(replicas=_max)
                    last_result[service] = _max
                    time_sclices[service] = self.max_time_sclice      
                    
                self.__logger.info(f'time_sclice: {time_sclices[service]}, st_ret: {st_ret}, ' 
                                   f'replicas: {last_result[service]}, scale_info: {scale_info} - {service}')          
                
            for service, conf in result.items():
                self.replicas[service] = conf
                            
            self.sync_replicas()

            control_loop_time = time.time() - control_loop_start
            if control_loop_time < self.ctrl_freq:
                time.sleep(self.ctrl_freq - control_loop_time)
    
    def lt_controller(self) -> Dict[str, int]:
        retval = dict()
        __lt_start = time.time()
        
        __tmp_logger = self.__logger.getChild('LTController')
        __tmp_logger.info('Running ...')
        
        # TODO not consider possible ceil
        idx = int(__lt_start - self.locust_start_time)
                        
        for dic in self.lt_pred_results:
            service = dic['service_name']
            # if service == 'price-service':
            #     print(idx)
            #     print(dic['predict_result'][idx])
            retval[service] = math.ceil(dic['predict_result'][idx] / self.cpu_usage_conf[service])
        return retval

    def st_controller(self) -> Dict[str, int]:
        retval = dict() 
        __st_start = time.time()

        __tmp_logger = self.__logger.getChild('STController')
        __tmp_logger.info('Running ...')
        
        ret_futures = list()
        results = list()

        locust_run_minutes = (time.time() - self.locust_start_time) // 60
        if locust_run_minutes < self.st_fetch_minutes:
            __tmp_logger.info(f'experiment has run {locust_run_minutes} minutes, st not ready')
            for service in self.get_all_services_from_cfg():
                retval[service] = -1
        else:
            __tmp_logger.info(f'experiment has run {locust_run_minutes} minutes')
            with futures.ThreadPoolExecutor() as executor:
                for service in self.get_all_services_from_cfg():
                    service_history_data = self.fetch_data(service)
                    short_term_pred = ShortTermPred(self.__cfg, self.__logger, service_history_data)
                    future = executor.submit(short_term_pred.predict, service)
                    ret_futures.append(future)
            
                for future in ret_futures:
                    results.append(future.result())
            
            self.st_pred_results = results
            
            for dic in self.st_pred_results:
                service = dic['service_name']
                retval[service] = math.ceil(dic['predict_result'] / self.cpu_usage_conf[service])
        
        return retval
    
    def fetch_data(self, service_name):
        # TODO POD NAME
        end = time.time()
        start = end - self.st_fetch_minutes * 60 + 1
        dep_name = self.get_k8s_dep_name_from_cfg(service_name)
        namespace = self.namespace
        
        fetch = self.get_cpu_usage_from_prom(
            dep_name=dep_name, 
            namespace=namespace,
            start=start,
            end=end)
        
        fetch = fetch['value'].values 
        prev = len(fetch)
        latter = self.st_history_len
        
        groups = [fetch[i:i+prev//latter] for i in range(0, prev, prev//latter)]
        group_means = [np.mean(group) for group in groups]
        
        # print(f'fet:{fetch}, len:{len(fetch)}')
        # print(f'groups:{groups}, len:{len(groups)}')
        # print(f'group_means:{group_means}, len:{len(group_means)}')
        return group_means


    def _predict_cpu_usage(self, service_name):
        assert service_name is not None
        
        self.__logger.info(f'predicting cpu_usage of {service_name}')
        
        # TODO get data
        data = self.fetch_data()
        st_predictor = ShortTermPred(data)
        retval = st_predictor.predict()
        
        result = dict(service_name=service_name)
        result.append(retval)
        
        return result
        
    
    def predict_cpu_usage(self):
        ret_futures = list()
        results = list()
        
        with futures.ThreadPoolExecutor() as executor:
            for service in self.get_all_services_from_cfg():
                # endpoints = self.get_service_endpoints(service)
                # for endpoint in endpoints:
                future = executor.submit(self._predict_cpu_usage, service)
                ret_futures.append(future)
            for future in ret_futures:
                results.append(future.result())
        
        return results
    
    def __do_sync_replicas(self, service, replicas_conf):
        __logger = self.__logger.getChild("__do_sync_replicas")
        
        replicas = replicas_conf.get('replicas') 
        
        dep_name = self.get_k8s_dep_name_from_cfg(service)
        namespace = self.get_k8s_namespace_from_cfg()
        
        dep_obj = self.get_k8s_deployment(dep_name, namespace)
        if dep_obj.spec.replicas != replicas: 
            dep_obj = self.set_k8s_deployment_replicas(dep_name, namespace, replicas) 
    
    def sync_replicas(self):        
        ret_futures = list()
        
        with futures.ThreadPoolExecutor() as executor:
            for service, replicas_conf in self.replicas.items():
                ret_futures.append(
                    executor.submit(self.__do_sync_replicas, service, replicas_conf)
                )
        
        for future in ret_futures:
            future.result()



class LongTermPred(object):
    
    def __init__(self, cfg, logger, service_history_data):
        self.__logger = logger.getChild('Main')
                
        self.data = service_history_data
        
        self.T = cfg.scaler.ahpa_scaler.T
        self.H = cfg.scaler.ahpa_scaler.H
        self.lambda_1 = cfg.scaler.ahpa_scaler.lambda_1
        self.lambda_2 = cfg.scaler.ahpa_scaler.lambda_2
        self.delta_d = cfg.scaler.ahpa_scaler.delta_d
        self.delta_i = cfg.scaler.ahpa_scaler.delta_i
        
        self.forecast_length = cfg.scaler.ahpa_scaler.forecast_length
        
        self.quantile = cfg.scaler.ahpa_scaler.alpha
        # self.quantile = cfg.scaler.ahpa_scaler.quantile
        self.trim_left = cfg.scaler.ahpa_scaler.trim_left
        self.trim_right = cfg.scaler.ahpa_scaler.trim_right
        
        self.guessd_delay = cfg.scaler.ahpa_scaler.guessd_delay
        self.throughput = cfg.scaler.ahpa_scaler.throughput


    
    def bilateral_filter(self, time_series, delta_d, delta_i, H):
        N = len(time_series)
        filtered_series = np.zeros(N)
        for t in range(N):
            weights = np.exp(-((np.arange(max(0, t-H), min(N, t+H+1)) - t) ** 2) / (2 * delta_d ** 2)) \
                    * np.exp(-((time_series[max(0, t-H):min(N, t+H+1)] - time_series[t]) ** 2) / (2 * delta_i ** 2))
            filtered_series[t] = np.sum(weights * time_series[max(0, t-H):min(N, t+H+1)]) / np.sum(weights)
        return filtered_series

    def robust_trend_extraction(self, time_series, lambda_1, lambda_2, T):
        N = len(time_series)
        X = np.arange(N)
        y = time_series

        # Trend variables
        L = cp.Variable(N)

        # Piecewise-linear trend fit
        fit = cp.abs(y - L)
        fit = cp.sum(fit)

        # Regularization terms
        reg1 = cp.norm(L[1:] - L[:-1], 1)
        reg2 = cp.norm(L[T:] - L[:-T], 1)

        # Objective
        objective = cp.Minimize(fit + lambda_1 * reg1 + lambda_2 * reg2)
        constraints = []

        # Problem
        prob = cp.Problem(objective, constraints)
        prob.solve(solver=cp.ECOS)

        return L.value

    def seasonal_adjustment(self, time_series, trend, T):
        adjusted_series = time_series - trend
        seasonal = np.zeros_like(time_series)
        for t in range(T):
            idx = np.arange(t, len(time_series), T)
            seasonal[idx] = np.mean(adjusted_series[idx])
        return seasonal  # This is the actual seasonal component


    def apply_robustSTL(self, time_series, delta_d, delta_i, H, lambda_1, lambda_2, T):
        # Bilateral filter to remove noise
        filtered_series = self.bilateral_filter(time_series, delta_d, delta_i, H)

        # Robust extraction of trend
        trend = self.robust_trend_extraction(filtered_series, lambda_1, lambda_2, T)

        # Seasonal adjustment to extract seasonal component
        seasonal = self.seasonal_adjustment(time_series, trend, T)

        # Calculation of remainder component
        remainder = time_series - trend - seasonal

        # print('trend', trend.shape, 'seasonal', seasonal.shape, 'remainder', remainder.shape)
        return trend, seasonal, remainder    
    
    def predict_without_shift(self, time_series):
        
        # RobustSTL decomposition
        trend, seasonal, remainder = self.apply_robustSTL(
            time_series, 
            T=self.T, 
            H=self.H, 
            lambda_1=self.lambda_1, 
            lambda_2=self.lambda_2, 
            delta_d=self.delta_d, 
            delta_i=self.delta_i
        )
            
        # Remainder prediction
        train_length = len(time_series)
        forecast_length = self.forecast_length
        ts = np.arange(1, train_length + 1)
        y = time_series
        X = sm.add_constant(ts)
        quantile = 0.1
        quantreg_model = sm.QuantReg(y, X).fit(q=quantile)
        quantile_forecast_time = np.arange(train_length + 1, train_length + forecast_length + 1)
        quantile_forecast_time = sm.add_constant(quantile_forecast_time)
        remainder_pred = quantreg_model.predict(quantile_forecast_time)

        forecast = seasonal[-forecast_length:] + trend[-forecast_length:] + remainder_pred[-forecast_length:]
        
        return forecast

    def forecast_shifting(self, 
                          probabilistic_forecast, 
                          guessed_delay, 
                          throughput, 
                          current_host_count):
        n = len(probabilistic_forecast)
        adjusted_forecast = probabilistic_forecast

        # 1. Adjust the forecast backwards to account for throughouput limitations
        for t in range(n - 2, -1, -1):
            adjusted_forecast[t] = max(adjusted_forecast[t], adjusted_forecast[t + 1] - throughput)

        # 2. Compute positive and negative adjustments
        delta = [adjusted_forecast[0] - current_host_count]
        # print(delta, current_host_count)
        for t in range(1, n):
            delta.append(adjusted_forecast[t] - adjusted_forecast[t - 1])

        requests, releases = [], []
        for d in delta:
            if d > 0:
                requests.append(d)
                releases.append(0)
            else:
                requests.append(0)
                releases.append(-d)

        # 3. Shift requests in the past according to the expected delay
        shifted_requests = [0] * n
        for t in range(n):
            shift_index = t - guessed_delay
            if shift_index >= 0:
                shifted_requests[shift_index] = requests[t]
                # # 避免冲突
                # releases[shift_index] = 0

        return shifted_requests, releases    
    
    def predict(self, service_name):
        __logger = self.__logger.getChild("Longpred")
        
        # 0. Get data
        data = self.data
        
        # 1. Predict next 14400 seconds
        forecast = self.predict_without_shift(data)

        # 2. Time shifting
        guessd_delay = self.guessd_delay
        throughput = self.throughput
        current_host_count = forecast[0]
        shifted_requests, releases = self.forecast_shifting(forecast, 
                                                            guessd_delay, 
                                                            throughput, 
                                                            current_host_count)

        actions = []
        forecast_shifted = []
        for i in range(len(shifted_requests)):
            actions.append(shifted_requests[i] - releases[i])
            forecast_shifted.append(current_host_count + np.sum(actions[:i]))
        
        # 3. Trimming
        forecast_shifted = forecast_shifted[self.trim_left: -self.trim_right]
        
        result = dict(service_name=service_name)
        result['predict_result'] = forecast_shifted        
        
        print(f'{service_name} has finish')        
        return result
    
class ShortTermPred(object):
    
    def __init__(self, cfg, logger, service_history_data):
        self.data = service_history_data
        self.__logger = logger
        
        self.lam1 = cfg.scaler.ahpa_scaler.lam1
        self.lam2 = cfg.scaler.ahpa_scaler.lam2
        self.gamma = cfg.scaler.ahpa_scaler.gamma
        self.st_history_len = cfg.scaler.ahpa_scaler.st_history_len
        self.alpha = cfg.scaler.ahpa_scaler.alpha



    def robust_trend(self, data, lam1, lam2, gamma, solver='ECOS'):
        """
        RobustTrend algorithm using cvxpy.
        :param y: Observed time series data (array-like).
        :param lam1: Regularization parameter for the first order difference.
        :param lam2: Regularization parameter for the second order difference.
        :param gamma: The Huber loss parameter.
        :return: Estimated trend component of the time series.
        """
        N = len(data)
        data = np.array(data)

        # # Construct difference matrices
        # D1 = np.eye(N, N-1, -1) - np.eye(N, N-1)
        # D2 = np.eye(N, N-2) - 2 * np.eye(N, N-2, -1) + np.eye(N, N-2, -2)
        # Construct difference matrices
        D1 = np.eye(N - 1, N, -1) - np.eye(N - 1, N)  # (N-1, N) matrix
        D2 = np.eye(N - 2, N) - 2 * np.eye(N - 2, N, -1) + np.eye(N - 2, N, -2)  # (N-2, N) matrix

        # Define cvxpy variables
        tau = cp.Variable(N)
        z1 = cp.Variable(N-1)
        z2 = cp.Variable(N-2)

        # Huber loss function
        huber = cp.sum(cp.huber(data - tau, gamma))

        # Objective function
        objective = cp.Minimize(huber + lam1 * cp.norm1(z1) + lam2 * cp.norm1(z2))

        # Constraints
        constraints = [D1 @ tau == z1, D2 @ tau == z2]

        # Problem definition and solving
        problem = cp.Problem(objective, constraints)
        if solver == 'ECOS':
            problem.solve(solver=cp.ECOS)
        else:
            problem.solve()
        return tau.value

    
    def predict(self, service_name:str, solver='ECOS'):
        __logger = self.__logger.getChild("Shortpred")
        
        # Run the RobustTrend algorithm
        
        trend = self.robust_trend(self.data, 
                                  self.lam1, 
                                  self.lam2, 
                                  self.gamma)
        # weights array from 1 to 0.1
        weights = np.linspace(1, 0.1, self.st_history_len)
        forecasted_trend_next_point = np.dot(weights, trend) / weights.sum()

        remainder = self.data - trend

        remainder_std = np.std(remainder)
        z_score = norm.ppf((1+self.alpha) / 2.0)  # percentile for the upper bound of a two-tailed test
        margin_of_error = z_score * remainder_std
        upper_bound = forecasted_trend_next_point + margin_of_error
        
        result = dict(service_name=service_name)
        result['predict_result'] = upper_bound        
        
        return result

    
    
    
