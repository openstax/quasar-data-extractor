import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import numpy as np

# Sample CSV data
data = """Days,0.25 vCPU,1 vCPU,2 vCPU,4 vCPU
1,0:01:32.791264,0:00:52.914511,0:00:55.794225,0:00:53.478424
2,0:02:22.299960,0:01:20.022596,0:01:23.388623,0:01:21.303763
5,0:04:27.317248,0:02:09.828812,0:02:09.734081,0:02:10.454933
10,0:08:12.888487,0:04:48.011605,0:04:57.964754,0:04:55.957366
20,0:16:02.681853,0:09:23.956856,0:09:39.486719,0:09:26.266069
30,0:22:33.458805,0:14:45.333282,0:14:09.238106,0:14:22.425311
"""

# Convert the string to a DataFrame
from io import StringIO
df = pd.read_csv(StringIO(data))

# Store original string format times
original_times = df.iloc[:, 1:].copy()

# Convert time to seconds
def time_to_seconds(t):
    h, m, s = t.split(':')
    return int(h) * 3600 + int(m) * 60 + float(s)

for column in df.columns[1:]:
    df[column] = df[column].apply(time_to_seconds)

# Plotting
fig, ax = plt.subplots(figsize=(10, 6))
for column in df.columns[1:]:
    # Scatter plot
    ax.scatter(df['Days'], df[column], label=column)

    # Linear regression
    model = LinearRegression()
    X = df[['Days']]
    y = df[column]
    model.fit(X, y)
    y_pred = model.predict(X)
    ax.plot(df['Days'], y_pred, linestyle='--')

# Modify y-ticks to display original time strings instead of seconds
y_ticks = ax.get_yticks()
y_ticklabels = []
for tick in y_ticks:
    hours = int(tick // 3600)
    minutes = int((tick % 3600) // 60)
    seconds = tick % 60
    y_ticklabels.append(f"{hours:02}:{minutes:02}:{seconds:02.0f}")
ax.set_yticklabels(y_ticklabels)

ax.set_xlabel('Days')
ax.set_ylabel('Time')
ax.set_title('Test Batch Filtering: started_session events\n Oct 2022 - Feb 2023, 100 specified users')
ax.legend()
plt.show()
