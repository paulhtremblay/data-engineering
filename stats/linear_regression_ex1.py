import numpy as np
from sklearn.linear_model import LinearRegression

def main():
    X = np.array([[1], [2], [3], [4]])
    y = np.array([2,4,6,8])
    reg = LinearRegression().fit(X, y)
    s = reg.score(X, y)
    coef = reg.coef_
    inter = reg.intercept_
    print(s)
    print(coef)
    print(inter)

def main_():
    X = np.array([[1, 1], [1, 2], [2, 2], [2, 3]])
    y = np.dot(X, np.array([1, 2])) + 3 
    print(y)
    reg = LinearRegression().fit(X, y)
    reg.score(X, y)
    reg.coef_
    reg.intercept_
    reg.predict(np.array([[3, 5]]))

if __name__ == '__main__':
    main()
