import time 
from typing import Tuple

import pandas as pd 
import numpy as np 
from gluonts.model.predictor import Predictor


class DeepARPredictor(object):
    def __init__(self, cfg) -> None:
        trained_model = cfg.ts_predictor.deepar.model
        
        # Load trained model 
        self.model: Predictor = Predictor.deserialize(trained_model)
        
    def fetch_data(self, service_name, endpoint_name, length) -> pd.DataFrame:
        sql = f"SELECT " \
              f"    (span_count * rt_mean) AS value, " \
              f"    time AS time " \
              f"FROM " \
              f"    {self.db_table} " \
              f"WHERE " \
              f"    service_name = '{service_name}' AND " \
              f"    endpoint_name = '{endpoint_name}' AND " \
              f"    window_size = 1000 " \
              f"ORDER BY " \
              f"    time DESC " \
              f"LIMIT " \
              f"    {length};"

        return self.sql_driver.exec(sql)
    
    def convert_pd_to_gluonts(self, data: pd.DataFrame):
        pass 
    
    def convert_forecast_to_numpy(self, forecasts) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        # TODO 
        pass 
        
    def predict(self, service_name, endpoint_name):
        # TODO fetch data 
        fetch_data_start = time.time()
        data = self.fetch_data()
        fetch_data_time = time.time() - fetch_data_start
        
        # TODO convert pandas data to gluonts data 
        data = self.convert_pd_to_gluonts(data)
        
        # TODO predict 
        forecasts = list(self.model.predict(data))
        
        # TODO convert forecasts to numpy 
        predict_value, upper_bound, lower_bound = self.convert_forecast_to_numpy(forecasts)
        
        return dict(
            predict_value=predict_value,
            upper_bound=upper_bound,
            lower_bound=lower_bound,
            stats=dict(
                fetch_data_time=fetch_data_time
            )
        )
        
