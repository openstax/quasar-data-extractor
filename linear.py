import numpy as np
import matplotlib.pyplot as plt  # To visualize
import pandas as pd  # To read data
from sklearn.linear_model import LinearRegression

data = pd.read_csv('times_c6in.csv')  # load data set
X = data.iloc[:, 0].values.reshape(-1, 1)  # values converts it into a numpy array
Y1 = data.iloc[:, 1].values.reshape(-1, 1)  # -1 means that calculate the dimension of rows, but have 1 column
Y2 = data.iloc[:, 2].values.reshape(-1, 1)  # -1 means that calculate the dimension of rows, but have 1 column
linear_regressor = LinearRegression()  # create object for the class
linear_regressor.fit(X, Y1)  # perform linear regression
Y1_pred = linear_regressor.predict(X)  # make predictions
linear_regressor.fit(X, Y2)  # perform linear regression
Y2_pred = linear_regressor.predict(X)  # make predictions

plt.scatter(X, Y1)
plt.scatter(X, Y2, color='blue')
plt.plot(X, Y1_pred, color='red')
plt.plot(X, Y2_pred, color='green')
plt.show()
