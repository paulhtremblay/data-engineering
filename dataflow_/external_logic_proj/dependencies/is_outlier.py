import numpy as np
import sklearn
from sklearn.linear_model import LinearRegression

class BadDataException(Exception):
    pass

def _make_array(X:list)-> list:
    """
    Converts a list to a list of lists (
    needed for sklearn)
    
    Arguments:
        X:list

    Returns:
      list of lists ([1, 2] => [[1], [2]])
    """
    l = []
    for i in range(len(X)):
        l.append([i])
    return l

def _get_residuals(model:sklearn.linear_model, X:np.array, y:list) -> list:
    """
    Gets the residuals of the fitted model

    Arguments:
       model: sklearn.linear_model object
       X: array of x values
       y: list of y values

    Returns:
        list of residuals
    """
    predictions = model.predict(X)
    return y - predictions

def _get_stdev(residuals:list)-> float:
    """
    gets the standard deviation

    Arguments:
       residuals: list of residuals

    Returns:
        float, the standard deviation

    """
    return np.std(residuals)

def _get_last_point(element:list, coef:float, intercept:float)-> float:
    """
    Gets the predictd value of the last point
    """
    last_point_X = len(element)
    return  last_point_X * coef + intercept

def _is_outside_3_std(y:float, predict_y:float, std:float)-> bool:
    """
    Determines if the last float is an outlier

    Arguments:
       y: actual y value
       predict_y: the predictd value based on linear regression
       std: standard deviation

    Returns:
        bool: True if y is equal to or greater than 3 deviations of the 
        predicted value
    """
    if  abs(y - predict_y) > 3 * std:
        return True
    return False

def _get_stats(element:list)->(list, list, float, float, float):
    """
    Returns the stats of linar regression, using the sklearn lib

    Arguments:
      element: list of floats

    Returns:
       X: array of x values
       y: list of y values
       model: the model, an object
       coef: the coeficients of the fit
       inter: the intercept of the fit
    """
    X = _make_array(element[0:-1])
    y = element[0:-1]
    reg = LinearRegression().fit(X, y)
    coef = reg.coef_
    inter = reg.intercept_
    return X, y, reg, coef, inter


def is_outlier(element:list)-> bool:
    """
    determins if the last number in a list
    is an outlier, based on on the standard deviation
    around the residuals.

    arguments:
      element: list of floats

    returns:
       True if the last element is outside 3 standard
       deviation of the residuals; otherwise False
    """
    assert isinstance(element, list)
    if len(element) < 2:
        raise BadDataException(f'element is len {len(element)}; not big enough')
    X, y, model, coef, intercept = _get_stats(element)
    residuals = _get_residuals(
            model = model,
            X = X,
            y = y)
    std = _get_stdev(residuals)
    predict_y = _get_last_point(
            element = element,
            coef = coef,
            intercept = intercept)
    return _is_outside_3_std(
            y = element[-1],
            predict_y = predict_y,
            std = std
            )
    
