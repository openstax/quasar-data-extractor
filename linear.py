import numpy as np
import matplotlib.pyplot as plt  # To visualize
import pandas as pd  # To read data
from sklearn.linear_model import LinearRegression

data = pd.read_csv('times_c6in.csv')  # load data set
X = data.iloc[:, 0].values.reshape(-1, 1)  # values converts it into a numpy array
X_name = data.iloc[:, 0].name
Y1 = data.iloc[:, 1].dropna().values.reshape(-1, 1)  # -1 means that calculate the dimension of rows, but have 1 column
Y1_name = data.iloc[:, 1].name
X1=X[:len(Y1)]
Y2 = data.iloc[:, 2].dropna().values.reshape(-1, 1)
Y2_name = data.iloc[:, 2].name
X2=X[:len(Y2)]
Y3 = data.iloc[:, 3].dropna().values.reshape(-1, 1)
Y3_name = data.iloc[:, 3].name
X3=X[:len(Y3)]

linear_regressor = LinearRegression()  # create object for the class
linear_regressor.fit(X1, Y1)  # perform linear regression
Y1_slope = linear_regressor.coef_[0][0]
Y1_pred = linear_regressor.predict(X)  # make predictions
linear_regressor.fit(X2, Y2)  # perform linear regression
Y2_slope = linear_regressor.coef_[0][0]
Y2_pred = linear_regressor.predict(X)  # make predictions
linear_regressor.fit(X3, Y3)  # perform linear regression
Y3_slope = linear_regressor.coef_[0][0]
Y3_pred = linear_regressor.predict(X)  # make predictions

plt.xlabel('Days Loaded')
plt.ylabel('Total Load Time (sec)')
plt.scatter(X1, Y1, color='pink', label=f'{Y1_name} (32 threads)')
plt.scatter(X2, Y2, color='lightgreen', label=f'{Y2_name} (128 threads)')
plt.scatter(X3, Y3, color='lightblue', label=f'{Y3_name} (1024 threads)')
#plt.plot(X1, Y1_pred, color='red', label = f'{Y1_slope:0.2f} sec/day')
plt.plot(X2, Y2_pred, color='green', label = f'{Y2_slope:0.2f} sec/day')
#plt.plot(X3, Y3_pred, color='blue', label = f'{Y3_slope:0.2f} sec/day')
plt.title('Test loading: started_session events\n Oct 2022 - Feb 2023, 100 specified users')
plt.legend()
plt.show()
