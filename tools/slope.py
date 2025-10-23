import matplotlib.pyplot as plt
import numpy as np
import os.path
import pandas as pd

from gooey import Gooey, GooeyParser

plt.rcParams['axes.grid'] = True

def plot_slope(group, x_var, outdir, groupby, save=True, plot_rmse=False):
    nrows = 2 if plot_rmse else 1
    fig, axes = plt.subplots(nrows=nrows, ncols=1, sharex=True, figsize=(12,8), squeeze=False)

    grouped = group.groupby(groupby)
    for g, data in grouped:
        print(g)
        marker_sizes = [ 20*n  for n in data['count'].values ]
        axes[0][0].scatter(x_var, 'slope', s=marker_sizes, label=g, data=data)
        if plot_rmse:
            axes[1][0].scatter(x_var, 'rmse', s=marker_sizes, label=g, data=data)
        else:
            axes[0][0].errorbar(x_var, 'slope', yerr='rmse', ls='None', elinewidth=1, capsize=1, data=data, label=None)

    if len(grouped) > 1:
        axes[0][0].legend()

    axes[0][0].set_title(group.name)
    axes[0][0].set_xlabel('s1_age')
    axes[0][0].set_ylabel('Raw Slope')

    if plot_rmse:
        axes[1][0].set_ylabel('RMSE')
        axes[1][0].set_xlabel(x_var.replace('_', ' ').title())

    if save:
        plt.savefig(os.path.join(outdir, '{}_raw_slopes.png'.format(group.name)))
    else:
        plt.show()


def calc_slope(infile, x_var, outfile=None, variables=None, groupby=None, just_normed_slopes=False, save_figs=True, plot_rmse=False):
    df = pd.read_csv(infile, index_col=[0,1])

    drop_cols = []
    if not groupby:
        groupby = 'temp_group'
        df[groupby] = 1
        drop_cols = groupby

    additional_cols = [ col for col in [x_var, groupby] if col ]

    if not variables:
        variables = [col for col in df.columns if col not in additional_cols ]

    print(variables)
    df = df.dropna(how='all', subset=variables)

    print(df)

    results = []
    for g, data in df.groupby(level=0):
        print(g)
        for var in variables:
            var_data = data[pd.notnull(data[var])]

            if len(var_data) <= 1:
                results.append([g, np.nan, np.nan, var, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])
                continue

            coeffs, resids, _, _, _ = np.polyfit(var_data[x_var], var_data[var], 1, full=True)
            resid = resids[0] if resids else 0
            rmse = np.sqrt(resid / len(var_data[x_var]))
            nrmse = rmse / (var_data[var].max() - var_data[var].min())
            x_var_range = var_data[x_var].max() - var_data[x_var].min()
            var_avg = np.average(var_data[var])
            slope_normed = 100.0*(coeffs[0]/var_avg)
            x_var_midpoint = var_data[x_var][0] + (x_var_range / 2.0)

            results.append([g, var_data[groupby][0], var_data[x_var][0], var, coeffs[0], rmse, nrmse, len(var_data), var_avg, x_var_range, x_var_midpoint, slope_normed])

    x_var = '_'.join([x_var, 's1'])
    columns = ['study_id', groupby, x_var, 'variable', 'slope', 'rmse', 'nrmse', 'count', 'avg', 'x_range', 'x_midpoint', 'percent_change_per_x_unit']
    slope_df = pd.DataFrame(results, columns=columns).set_index(['study_id', 'variable'])
    if not outfile:
        outfile = '{}_slopes.csv'.format(os.path.splitext(infile)[0])

    slope_df.groupby('variable').apply(plot_slope, x_var, os.path.dirname(outfile), groupby, save_figs, plot_rmse)

    slope_df = slope_df.drop(columns=drop_cols)
    # slope_df.to_csv('before_drop_cols.csv')
    if just_normed_slopes:
        slope_df = slope_df.drop(columns=['slope', 'rmse', 'nrmse', 'count', 'avg', 'x_range', x_var])
    if groupby in slope_df.columns:
        slope_df = slope_df.reset_index().set_index(['study_id', groupby, 'variable'])
    slope_df = slope_df.unstack().sort_index(1, level=1)
    slope_df.columns = [ '_'.join(map(str, reversed(col))) for col in slope_df.columns ]
    slope_df.to_csv(outfile)


@Gooey
def parse_args():
    parser = GooeyParser()
    required = parser.add_argument_group('Required Arguments', gooey_options={'columns':1})
    required.add_argument('--infile', required=True, widget='FileChooser', help='CSV file with variables of interest (assumes first 2 columns are some unique identifier for subject/session')
    required.add_argument('--x_var', required=True, help='column name to use for x-axis (i.e. wolf_age)')

    optional = parser.add_argument_group('Optional Arguments', gooey_options={'columns':1})
    optional.add_argument('--outfile', widget='FileChooser', help='name for outputted CSV file (default is <infile>_slope)')
    optional.add_argument('--variables', nargs='+', help='variables to calculate slope for (default is all in file -- other than x_var and sub/session identifiers))')
    optional.add_argument('--groupby', help='column name to use for labelling by group in plots')
    optional.add_argument('--just_normed_slope', action='store_true', help='only normed slopes in output csv')
    optional.add_argument('--show_only', action='store_true', help='only show the graphs (default is to save them to file)')
    optional.add_argument('--plot_rmse', action='store_true', help='plot RMSE as separate plot (default=error bars)')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    calc_slope(args.infile, args.x_var, args.outfile, args.variables, args.groupby, args.just_normed_slope, not args.show_only, args.plot_rmse)
