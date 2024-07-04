import matplotlib.pyplot as plt
import pandas as pd

def outer_index_barplot(df: pd.DataFrame,
                        filepath: str) -> None:
    
    fig, axs = plt.subplots(ncols=2,
                            squeeze=True,
                            sharey=True,
                            sharex=True,
                            figsize=(19.2,10.8))

    outer_levels = df.index.get_level_values(0).unique()

    if outer_levels.__len__() != 2:
        raise ValueError(f'outer_index_barplot supports only 2 level outer index objects: {outer_levels}')

    for (ax, outer_level) in zip(axs, outer_levels):

        df_xs = df.xs(outer_level, level=0)

        df_xs.index = df_xs.index.str[:-4]

        df_xs.plot(kind='bar',
                   ax=ax,
                   title=outer_level,
                   rot=60)
        
    plt.ylabel("Number of signups")
    plt.xlabel('Event')
    plt.suptitle('Most popular events')
    plt.tight_layout()
    fig.savefig(filepath)