import traceback

def get_n(n ):
    if n == 1:
        return n
    else:
        raise ValueError(f'{n} not allowed')

def main():
    try:
        get_n(1)
        get_n(2)
    except Exception as e:
        s = traceback.format_exc()
        print(type(s))
        return s


if __name__ == '__main__':
    main()

