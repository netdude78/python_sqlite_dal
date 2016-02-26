#!/usr/bin/python


def synchronized(func):
    """synchronized decorator function
    
    This method allows the ability to add @synchronized decorator to any method.
    This method will wrap the decorated function around a call to threading.Lock
    ensuring only a single execution of the decorated method/line of code at a time.
    Use for ensuring thread-safe DB operations such as inserts where a last_inserted_id
    type action is taking place.

    Arguments:
        func {*args, **kwargs} -- [anything passed to the func will be passed through]
    
    Returns:
        the wrapped method.
    """
    import threading
    func.__lock__ = threading.Lock()

    def synced_func(*args, **kws):
        with func.__lock__:
            return func(*args, **kws)
    return synced_func