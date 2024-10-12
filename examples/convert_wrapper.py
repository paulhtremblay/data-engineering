import inspect

def convert(func):
    def convert_wrapper(*args, **kwargs):
        def convert_to_str(x):
            return x.decode('utf8')
        l = []
        for i, arg_value in enumerate(args):
            if isinstance(arg_value, bytes):
                l.append(arg_value.decode('utf8'))
            else:
                l.append(arg_value)
        d = {}
        for keyword_name, arg_value in kwargs.items():
            if isinstance(arg_value, bytes):
                d[keyword_name] = arg_value.decode('utf8')
            else:
                d[keyword_name] = arg_value
        return func(*l, **d)

    return convert_wrapper


@convert
def print_msg(x):
    assert isinstance(x, str)


print_msg('ff')
print_msg(b'ff')
print_msg(x = b'ff')
print_msg(x = 'ff')
