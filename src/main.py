import argparse
import logging.config
from math import ceil

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.neighbors import KernelDensity

from .dune import get_slippage


def get_mode(solver, df):
    X = df[df.solver == solver].slippage.values.reshape(-1, 1)
    kde = KernelDensity(kernel='gaussian', bandwidth=1).fit(X)
    xx = np.arange(-15, 15, 0.01)
    mode = max(xx, key=lambda i: np.exp(kde.score([[i]])))
    return mode, kde


def plot(solver, df, mode, kde, ax):
    X = df[df.solver == solver].slippage.values.reshape(-1, 1)
    xx = np.arange(-15, 15, 0.01)
    yy = [np.exp(kde.score([[i]])) for i in xx]
    ax.plot(xx, yy)
    ax.vlines([mode], ymin=0, ymax=max(yy))
    ax.set_title(solver)


logging.config.fileConfig(fname='logging.conf', disable_existing_loggers=True)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Check if solvers are pennying."
    )

    parser.add_argument(
        'min_start_time',
        type=str,
        help="Minimum start time in the form of yyyy-mm-dd HH:MM."
    )

    parser.add_argument(
        'max_start_time',
        type=str,
        help="Minimum start time in the form of yyyy-mm-dd HH:MM."
    )

    args = parser.parse_args()


    r = get_slippage(args.min_start_time, args.max_start_time)
    df = pd.DataFrame.from_records(r)

    nr_solvers = df.solver.unique().shape[0]
    fig, axs = plt.subplots(ceil(nr_solvers/3), 3, figsize=(9, 9), sharey=True, sharex=True)

    for i, solver in enumerate(df.solver.unique()):
        mode, kde = get_mode(solver, df)
        print(solver, "mode :", mode)
        if mode <= -0.2:
            print("\tis pennying!")
        plot(solver, df, mode, kde, axs[(i // 3), (i % 3) ])

    fig.tight_layout()
    plt.show()
