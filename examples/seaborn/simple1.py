import pandas as pd
import random
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    penguins = pd.read_csv('penguins.csv')
    bar_plot = sns.barplot(penguins, x="island", y="body_mass_g")
    fig = bar_plot.get_figure()
    fig.savefig("out.png")

if __name__ == '__main__':
    main()
