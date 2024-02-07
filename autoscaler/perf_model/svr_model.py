import time 
from typing import Dict 
from logging import Logger 

from omegaconf import DictConfig
from sklearn.svm import SVR 
import pandas as pd 
import numpy as np 
from mapie.regression import MapieRegressor


STRATEGIES = {
    'jackknife_plus': dict(method='plus', cv=-1),
    'cv_plus': dict(method='plus', cv=10)
}

# 根据(服务，端点)的吞吐量预测其响应时间
class SVRPerfModel(object):
    def __init__(self, cfg: DictConfig, logger: Logger,
                 service_name, endpoint_name) -> None:
        
        self.__logger = logger.getChild('SVRPerfModel')
        self.__cfg = cfg 
        
        self.__service_name = service_name 
        self.__endpoint_name = endpoint_name 
        
        self.__base_regressor = SVR()
        self.__regressor = None 
        
    def _load_data(self, dataset: pd.DataFrame):
        
        # TODO Filter 
        dataset = dataset[dataset.service_name == self.__service_name]
        dataset = dataset[dataset.endpoint_name == self.__endpoint_name]
        dataset = dataset[dataset.window_size == 1000]
        
        rps = dataset.span_count.values 
        rt_mean = dataset.rt_mean.values 
        
        # TODO 
        return rps, rt_mean 
    
    def fit(self, data: pd.DataFrame) -> Dict:
        self.__logger.info('Fitting ...') 
        
        # Load training dataset 
        load_data_start = time.time()
        x_train, y_train = self._load_data(data)
        load_data_time = time.time() - load_data_start
        
        # TODO use cfg
        strategy_params = STRATEGIES.get('cv_plus')
        
        self.__regressor = MapieRegressor(
            self.__base_regressor,
            **strategy_params
        )
        
        # Fit the model
        fit_start = time.time() 
        self.__regressor = self.__regressor.fit(x_train, y_train)
        fit_time = time.time() - fit_start
        
        return dict(
            time_stats=dict(
                fit_time=fit_time,
                load_data_time=load_data_time
            )
        )
    
    def predict(self, x) -> Dict:
        if self.__regressor is None:
            raise Exception('Fit the SVR perf model first.') 
        
        # TODO set alpha using config file 
        predict_start = time.time()
        y_pred, y_pis = self.__regressor.predict(x, alpha=0.05)
        predict_time = time.time() - predict_start
        
        return dict(
            time_stats=dict(
                predict_time=predict_time
            )
        )

