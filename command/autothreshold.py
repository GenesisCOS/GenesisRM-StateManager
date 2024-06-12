import argparse 


class AutoThresholdCommand(object):
    def __init__(self):
        pass 
    
    def status(self):
        print('AutoThreshold status')
    
    def __call__(self, opt):
        if opt.status:
            self.status()  
    
    @staticmethod
    def get_argparser():
        parser = argparse.ArgumentParser()
        parser.add_argument('--status', action='store_true', dest='status', help='获取status')
        return parser