import matplotlib.pyplot as plt
import pandas as pd

def outer_index_barplot(df: pd.DataFrame,
                        filepath: str,
                        suptitle: str,
                        xlab: str = "",
                        nrows: int = 1,
                        ncols: int = 1) -> None:
    """
    Plot separate barplots for each outer level of a MultiIndex

    Arguments
    ---------
    df:
        A MultiIndex dataframe
    filepath:
        A string specifying where to save the figure
    suptitle:
        A string specifying the name of the plot
    xlab:
        A string specifying the x-axis label of each figure
    nrows, ncols:
        An integer specifying the number of rows/columns in the plot grid
    """
    
    fig, axs = plt.subplots(nrows=nrows,
                            ncols=ncols,
                            squeeze=True,
                            sharey=True,
                            figsize=(19.2,10.8))

    outer_levels = df.index.get_level_values(0).unique()

    if outer_levels.__len__() != len(axs):
        raise ValueError(f'The number of outer index levels and subplots are not equal: '
                         'Expected {outer_levels.__len__()}, got {len(axs)}')

    for (ax, outer_level) in zip(axs, outer_levels):

        # Extract cross-section for a given outer index level
        df_xs = df.xs(outer_level, level=0)

        df_xs.plot(kind='bar',
                   ax=ax,
                   title=outer_level,
                   rot=60,
                   xlabel=xlab)
        
    plt.suptitle(suptitle)
    plt.tight_layout()
    fig.savefig(filepath)