from typing import Dict 
import argparse 

from rich import print as rich_print
import requests 

from .utils import get_rich_table


class AutoWeightCommand(object):
    def __init__(self, cfg):
        self.__host = cfg.autoweight.host  
    
    def status(self) -> Dict | None:
        return None 
        return dict(
            weights=str([1, 2, 3]),
            run_mode='learn'
        )
        resp = requests.get(f'http://{self.__host}/api/v1/status')
        try:
            return resp.json()
        except:
            return None 
    
    def __call__(self, opt):
        if opt.status:
            status = self.status() 
            if status is None:
                rich_print('[bold red]获取 status 失败[/] :pile_of_poo:')
            else:
                table = get_rich_table()
                for i in status.keys():
                    table.add_column(i)
                table.add_row(*status.values(), style='red')
                rich_print(table)
    
    @staticmethod
    def get_argparser():
        parser = argparse.ArgumentParser()
        parser.add_argument('--status', action='store_true', dest='status', help='获取status')
        return parser
