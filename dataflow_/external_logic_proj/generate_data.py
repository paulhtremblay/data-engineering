import random
import numpy as np
import pprint
pp = pprint.PrettyPrinter(indent =4)

def main():
    keys = [('dept1', 1, 2.1), ('dept2', 1.5, 2.2), ('dept3', 1.9, 2.3)]
    t = (('dept1',[]), ('dept2',[]), ('dept3',[]))
    for counter, key in enumerate(keys):
        for i in range(30):
            t[counter][-1].append(round(i * key[2] + key[1] + np.random.normal(),1))
    t[2][1][-1] = t[2][1][-1] + 5

    pp.pprint(t)

if __name__ == '__main__':
    main()
