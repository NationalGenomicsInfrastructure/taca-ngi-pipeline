""" Utility functions related to logging """
import logging
from taca_ngi_pipeline import __version__

def getLogger(name):
    """ Get a logger instance which appends the plugin version to log 
        messages
        
        :param string name: a name that will appear before log messages written 
            by this logger instance  
        :returns: a logger instance
    """
    
    return logging.getLogger("{}[{}]".format(name,__version__))
    