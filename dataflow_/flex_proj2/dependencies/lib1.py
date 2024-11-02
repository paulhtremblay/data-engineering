TEST_VAR2 = 'TEST2'
import logging

def func1(element, verbosity = 0):
    logging.debug('in debug func1')
    logging.info('in func1')
    logging.error('in func1')
    return element
