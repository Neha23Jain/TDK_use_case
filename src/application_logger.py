    
import os
from datetime import datetime
import logging
import warnings
from importlib import reload

#Ignore warnings generated in script

warnings.filterwarnings('ignore')

def define_logger(log_file_nm):
    #define loggers
    try:
        #reset logger
        logging.shutdown()
        from imp import reload 
        reload(logging)

        #create and configure logger
        #date_timestamp=datetime.now().strftime("%d-%m-%Y_%H:%M:%S:%f")
        #file=log_file_nm+"_"+date_timestamp+".log"
        logging.basicConfig(filename=log_file_nm, format='%(asctime)s %(message)s',filemode='w')
        logger=logging.getLogger()
        logger.setLevel(logging.INFO)

        return logger

    except Exception as error:
        print('Unable to find logger -\n',error)