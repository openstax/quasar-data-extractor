#!/usr/bin/env python3

import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import numpy as np
import argparse
import os
from matplotlib.ticker import FixedLocator
import codecs

def time_to_seconds(t):
    if pd.isna(t):
        return None
    h, m, s = t.split(':')
    return int(h) * 3600 + int(m) * 60 + float(s)

def plot_from_csv(csv_path, plot_title):
    df = pd.read_csv(csv_path)

    for column in df.columns[1:]:
        df[column] = df[column].apply(time_to_seconds)

    # Plotting
    fig, ax = plt.subplots(figsize=(10, 6))
    for column in df.columns[1:]:
        non_na_rows = df.dropna(subset=[column])
        ax.scatter(non_na_rows['Days'], non_na_rows[column], label=column)

        model = LinearRegression()
        X = non_na_rows[['Days']]
        y = non_na_rows[column]
        model.fit(X, y)
        y_pred = model.predict(X)
        ax.plot(non_na_rows['Days'], y_pred, linestyle='--')

    y_ticks = ax.get_yticks()
    y_ticklabels = []
    for tick in y_ticks:
        hours = int(tick // 3600)
        minutes = int((tick % 3600) // 60)
        seconds = tick % 60
        y_ticklabels.append(f"{hours:02}:{minutes:02}:{seconds:02.0f}")

    ax.yaxis.set_major_locator(FixedLocator(y_ticks))
    ax.set_yticklabels(y_ticklabels)

    ax.set_xlabel('Days')
    ax.set_ylabel('Time')
    ax.set_title(plot_title)
    ax.legend()
    plt.show()

def main():
    parser = argparse.ArgumentParser(description='Plot CSV data with an optional title.')
    parser.add_argument('csv_path', type=str, help='Path to the CSV file')
    parser.add_argument('-t', '--title', type=str, help='Title for the plot')
    
    args = parser.parse_args()

    # If a title is provided, interpret escape sequences
    if args.title is not None:
        args.title = codecs.decode(args.title, 'unicode_escape')

    # If no title provided, set default title with file name
    else:
        args.title = f"Scatter Plot for {os.path.basename(args.csv_path)}"

    plot_from_csv(args.csv_path, args.title)

if __name__ == '__main__':
    main()
