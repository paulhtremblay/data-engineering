"""
TODO: determine how the topics will be read at runtime (probably from a database)

"""

def get_topics(runner:str) -> list:
    """
    determines the topics to use for apache beam job
    
    :param: runner str: the runner: choices are DirectRunner and DataflowRunner

    :returns: A list of topics
    
    :raises: NotImplementedError if runner is not supported

    """
    if runner == 'DirectRunner':
        return ['ingest_test']
    else:
        raise NotImplementedError(f'runner {runner} not supported')

