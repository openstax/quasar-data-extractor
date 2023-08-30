import numpy as np
import matplotlib.pyplot as plt  # To visualize
import pandas as pd  # To read data
from sklearn.linear_model import LinearRegression

data = pd.read_csv('times_c6in.csv')  # load data set
X = data.iloc[:, 0].values.reshape(-1, 1)  # values converts it into a numpy array
X_name = data.iloc[:, 0].name
Y1 = data.iloc[:, 1].values.reshape(-1, 1)  # -1 means that calculate the dimension of rows, but have 1 column
Y1_name = data.iloc[:, 1].name
Y2 = data.iloc[:, 2].values.reshape(-1, 1)
Y2_name = data.iloc[:, 2].name
Y3 = data.iloc[:, 3].values.reshape(-1, 1)
Y3_name = data.iloc[:, 3].name
linear_regressor = LinearRegression()  # create object for the class
linear_regressor.fit(X, Y1)  # perform linear regression
Y1_slope = linear_regressor.coef_[0][0]
Y1_pred = linear_regressor.predict(X)  # make predictions
linear_regressor.fit(X, Y2)  # perform linear regression
Y2_slope = linear_regressor.coef_[0][0]
Y2_pred = linear_regressor.predict(X)  # make predictions
linear_regressor.fit(X, Y3)  # perform linear regression
Y3_slope = linear_regressor.coef_[0][0]
Y3_pred = linear_regressor.predict(X)  # make predictions

plt.xlabel('Days Loaded')
plt.ylabel('Total Load Time (sec)')
plt.scatter(X, Y1, color='pink', label=f'{Y1_name} (32 threads)')
plt.scatter(X, Y2, color='lightgreen', label=f'{Y2_name} (128 threads)')
plt.scatter(X, Y3, color='lightblue', label=f'{Y3_name} (1024 threads)')
plt.plot(X, Y1_pred, color='red', label = f'{Y1_slope:0.2f} sec/day')
plt.plot(X, Y2_pred, color='green', label = f'{Y2_slope:0.2f} sec/day')
plt.plot(X, Y3_pred, color='blue', label = f'{Y3_slope:0.2f} sec/day')
plt.title('Test loading: started_session events\n Nov 2022, 100 specified users')
plt.legend()
plt.show()
