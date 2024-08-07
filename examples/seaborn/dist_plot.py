import pandas as pd
import random
import matplotlib.pyplot as plt
import seaborn as sns

def main():
    clust = [65, 89, 65, 45, 45, 56, 58, 63, 52, 59, 68, 68, 42, 37, 45, 86, 37, 59, 51, 46, 60, 39, 65, 43, 64, 36, 34, 61, 58, 38]
    no_clust = [830, 865, 646, 667, 679, 946, 689, 803, 862, 649, 867, 798, 793, 753, 772, 942, 738, 615, 808, 854, 639, 795, 795, 902, 909, 703, 770, 679, 788, 846]

    df = pd.DataFrame(data = {'clust':clust, 'no-clust':no_clust})
    df_clust = pd.DataFrame(data = {'clust':clust})
    df_no_clust = pd.DataFrame(data = {'no-clust':no_clust})
    sns.set_style('whitegrid')
    print(df)
    return
    #hist_plot = sns.histplot(df['amt'], kde = False, color ='red', bins = 10)
    #plt.hist([x, y], color=['r','b'], alpha=0.5)
    hist_plot = sns.histplot(
            [df['no-clust'], df['clust']], 
            kde = False, color =['blue', 'red'], bins = 30,
            stat = 'density'
            )

    fig = hist_plot.get_figure()
    fig.savefig("out.png")

    return

    penguins = pd.read_csv('penguins.csv')
    bar_plot = sns.barplot(penguins, x="island", y="body_mass_g")
    fig = bar_plot.get_figure()
    fig.savefig("out.png")

if __name__ == '__main__':
    main()
