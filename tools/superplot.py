import json
import matplotlib.pyplot as plt
import numpy as np
import os.path
import pandas as pd
import re

from gooey import Gooey, GooeyParser
from matplotlib import lines, markers
from scipy import stats


DEFAULT_LINE_OPTS = {
    'color': 'black'
}
DEFAULT_MARKER_OPTS = {
    'markeredgecolor': 'black',
    'markerfacecolor': 'white',
    'color': 'black'
}
LINE_STYLES = list(lines.lineStyles.keys())
MARKER_STYLES = list(lines.Line2D.filled_markers)

# helper function to build style options by iterating over possible linestyles and markers for each group
def build_style_opts(idx):
    style_opts = {
        'markerstyle': {
            'marker': MARKER_STYLES[idx]
        },
        'linestyle': {
            'linestyle': LINE_STYLES[idx]
        }
    }
    style_opts['markerstyle'].update(DEFAULT_MARKER_OPTS)
    style_opts['linestyle'].update(DEFAULT_LINE_OPTS)
    return style_opts
   

def plot_subject(data, xvar, yvar, style={}):
    plt.plot(data[xvar], data[yvar], **style)
    # sns.lineplot(x=xvar, y=yvar, data=data, **style)


def superplot(datafile, xvar, style_config=None, groupby=None, vars=[], min_pval=1, ylabel=None, outdir=None):
    df = pd.read_csv(datafile, index_col=[0,1])

    style_opts = {}
    if style_config:
        with open(style_config) as f:
            style_opts = json.load(f)

    if not outdir:
        outdir = os.path.dirname(datafile)
    outfile_root = os.path.join(outdir, os.path.basename(datafile).split('.')[0]) 

    if not vars:
        vars = [ col for col in df.columns if col not in [xvar, groupby] ] # if no subset of variables is given, plot everything except index cols + already used vars
    else:
        vars = [ col for var in vars for col in df.columns if re.search(var, col) ] # else, get dataframe columns that match column names / regexes
    
    if groupby:
        grp = df.groupby(groupby)

        scatter_legend = []
        for idx, g in enumerate(grp.groups):
            if g not in style_opts:
                print('No style spec found for label: {}; using default styling...'.format(g))
                style_opts[g] = build_style_opts(idx)
            scatter_legend.append(lines.Line2D([], [], **(style_opts[g]['markerstyle']), label=g))

        for yvar in vars:
            line_legend = []

            for g, data in grp:
                for _, subdata  in data.groupby(level=0):
                    plot_subject(subdata, xvar, yvar, style_opts[g]['markerstyle'])

                reg = stats.linregress(data[xvar], data[yvar])
                rval, pval = stats.pearsonr(data[xvar], data[yvar])
                mat = np.corrcoef(data[xvar], data[yvar])
                label = '{0} (R^2 = {1:.2f}, p = {2:.2f})'.format(g, rval**2, pval)
                if reg.pvalue < min_pval:
                    new_x = range(int(data[xvar].min()), int(data[xvar].max())+1)
                    plt.plot(new_x, reg.slope * new_x + reg.intercept, **(style_opts[g]['linestyle']))
                    line = lines.Line2D([0], [0], **(style_opts[g]['linestyle']), label=label)
                else:
                    line = lines.Line2D([0], [0], color='white', label=label)
                line_legend.append(line)

            plt.legend(handles=scatter_legend + line_legend, bbox_to_anchor=(1.02, 0.5), loc='center left', prop={'size': 6})
            plt.subplots_adjust(right=0.75)
            plt.title(' '.join(yvar.split('_')))
            plt.xlabel(' '.join(xvar.split('_')))

            if ylabel:
                plt.ylabel(ylabel)

            outfile = '{}_{}.tif'.format(outfile_root, yvar)
            plt.savefig(outfile, dpi=300, format='tiff', bbox_inches='tight')
            plt.close()



@Gooey(tabbed_groups=True)
def parse_args():
    parser = GooeyParser()
    
    req = parser.add_argument_group('Required arguments')
    req.add_argument('datafile', widget='FileChooser', help='CSV file with one row per session w/ unique subject + session identifier as first 2 columns')
    req.add_argument('xvar', help='columns in datafile to use on x-axis')

    fmt = parser.add_argument_group('Formatting options')
    fmt.add_argument('--style-config', widget='FileChooser', help='JSON file containing style properties for each group')
    fmt.add_argument('--groupby', help='column in datafile to split groups on')
    fmt.add_argument('--columns', nargs='+', help='column names (or regexes) to select subset of columns to plot (default is all)')
    fmt.add_argument('--pval', type=float, default=0.05, help='only plot cross-sectional trend line if pval less than value')
    fmt.add_argument('--ylabel', help='shared label for y-axis (i.e. mm^3, % ICV)')

    opt = parser.add_argument_group('Optional arguments')
    opt.add_argument('--outdir', widget='DirChooser', help='where to store plots (default is same directory as datafile)')

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    superplot(args.datafile, args.xvar, args.style_config, args.groupby, args.columns, args.pval, args.ylabel, args.outdir)