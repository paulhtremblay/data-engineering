import pandas as pd
import random
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    name = ['cluster', 'no-cluster']
    seconds = [90,35]
    df = pd.DataFrame(data = {'type':name, 'seconds': seconds})
    bar_plot = sns.barplot(df, x="type", y="seconds")
    fig = bar_plot.get_figure()
    fig.savefig("out.png")

if __name__ == '__main__':
    main()
