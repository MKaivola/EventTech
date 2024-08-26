import os

import matplotlib.pyplot as plt
import pandas as pd


def outer_index_barplot(
    df: pd.DataFrame,
    filename: str,
    suptitle: str,
    config: dict[str, str],
    df_line_plot: pd.DataFrame = None,
    s3_client=None,
    xlab: str = "",
    nrows: int = 1,
    ncols: int = 1,
) -> None:
    """
    Plot separate barplots for each outer level of a MultiIndex
    Optionally, save the figure to AWS S3 bucket

    Arguments
    ---------
    df:
        A MultiIndex dataframe
    filename:
        A string specifying the filename of the figure
    suptitle:
        A string specifying the name of the plot
    config:
        Storage configuration dictionary
    df_line_plot:
        Optional MultiIndex dataframe for line plots
    s3_client:
        AWS S3 client object
    xlab:
        A string specifying the x-axis label of each figure
    nrows, ncols:
        An integer specifying the number of rows/columns in the plot grid
    """

    fig, axs = plt.subplots(
        nrows=nrows, ncols=ncols, squeeze=True, sharey=True, figsize=(19.2, 10.8)
    )

    outer_levels = df.index.get_level_values(0).unique()

    if outer_levels.__len__() != len(axs):
        raise ValueError(
            "The number of outer index levels and subplots are not equal: "
            f"Expected {outer_levels.__len__()}, got {len(axs)}"
        )

    for ax, outer_level in zip(axs, outer_levels):
        # Extract cross-section for a given outer index level
        df_xs = df.xs(outer_level, level=0)

        df_xs.plot(kind="bar", ax=ax, title=outer_level, rot=60, xlabel=xlab)

        if df_line_plot is not None:
            df_xs_line = df_line_plot.xs(outer_level, level=0)

            ax.plot(df_xs_line.index - 1.0, df_xs_line, "ro")

    plt.suptitle(suptitle)
    plt.tight_layout()

    local_plot_dir = config["local_plot_dir"]

    full_path = "".join([local_plot_dir, "/", filename])
    fig.savefig(full_path)

    if s3_client is not None:
        s3_upload(full_path, config, s3_client)


def barplot(
    df: pd.DataFrame, title: str, filename: str, config: dict[str, str], s3_client=None
) -> None:
    """
    Plot a single barplot based on a pandas dataframe
    Optionally, save the figure to AWS S3 bucker

    Arguments
    ---------
    df:
        pandas dataframe
    title:
        Title of the figure
    filename:
        A string specifying the filename
    config:
        Storage configuration dictionary
    s3_client:
        AWS S3 client object
    """

    local_plot_dir = config["local_plot_dir"]

    full_path = "".join([local_plot_dir, "/", filename])

    # First save locally
    df.plot(kind="bar", title=title).figure.savefig(full_path)

    if s3_client is not None:
        s3_upload(full_path, config, s3_client)


def s3_upload(local_path: str, config: str, s3_client) -> None:
    """
    Upload locally saved object to AWS S3 bucket

    Arguments
    ---------
    local_path:
        Absolute path to locally stored object
    config:
        Storage configuration dictionary
    s3_client:
        AWS S3 client object
    """

    bucket_name = config["s3_bucket_name"]

    full_path_bucket = "".join(
        [config["s3_plot_dir"], "/", os.path.basename(local_path)]
    )

    s3_client.upload_file(Filename=local_path, Bucket=bucket_name, Key=full_path_bucket)
