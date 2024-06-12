import sys 
import argparse

import hydra 
from omegaconf import DictConfig
import cmd2 
from cmd2 import with_argparser

from .autoweight import AutoWeightCommand
from .autothreshold import AutoThresholdCommand

class GenesisCMD(cmd2.Cmd):
    autoweight_parser = AutoWeightCommand.get_argparser()
    autothreshold_parser = AutoThresholdCommand.get_argparser()
    
    def __init__(self, cfg):
        self.__cfg = cfg
        super().__init__() 
        
    @with_argparser(autoweight_parser)
    def do_autoweight(self, opt):
        AutoWeightCommand(self.__cfg)(opt)
    
    @with_argparser(autothreshold_parser) 
    def do_autothreshold(self, opt):
        AutoThresholdCommand(self.__cfg)(opt)
        
        
@hydra.main(version_base=None, config_path='../config', config_name='config')
def main(cfg: DictConfig) -> None:
    c = GenesisCMD(cfg.cmd)
    sys.exit(c.cmdloop())


if __name__ == '__main__':
    sys.exit(main())
