def get_methods(o:object):
    l = []
    for i in dir(o):
        if i.startswith('_'):
            continue
        l.append(i)
    return l

