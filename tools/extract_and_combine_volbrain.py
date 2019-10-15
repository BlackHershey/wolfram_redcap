import argparse
import os
import pandas as pd
import re
import shutil

from glob import glob
from gooey import Gooey, GooeyParser
from zipfile import ZipFile

def get_processed_jobs(report):
    df = pd.read_csv(report, index_col=[0,1,2,3])
    df.columns = df.columns.str.split(' - ', n=1, expand=True)
    df = df.stack(level=1).reset_index()
    return dict(zip(df['job_id'], df['level_4']))


def save_report(df, outfile, redo):
    if redo and os.path.exists(outfile):
        old_df = pd.read_csv(outfile).set_index(['Subject', 'Patient ID', 'Age', 'Sex', 'type']).sort_index()
        df = pd.concat([old_df, df])
        df = df[~df.index.duplicated(keep='last')] # drop duplicate indexes + take most recently appended
    df.to_csv(outfile)
    return df


def extract_and_combine(indir, outdir, patid_pattern, rename=False, redo=False, remove_zips=False):
    # extract/rename files in volbrain zips
    job_search = re.compile('(job\d{6})')

    patid_search = re.compile('({})'.format(patid_pattern), flags=re.IGNORECASE)

    zips = glob(os.path.join(args.indir, '*.zip'))

    native_zips = [ f for f in zips if 'native' in f ]
    mni_zips = [ f for f in zips if f not in native_zips ]

    # create map of job id to report type in case mni has already been processed but native has not
    master_report = os.path.join(outdir, 'volbrain_master_report.csv')
    job_map = get_processed_jobs(master_report) if os.path.exists(master_report) else {}

    report_dfs = {}
    for zip_file in mni_zips + native_zips: # make sure we're always iterative over the mni ones first (need lookup table for report type)
        print('Processing ', zip_file)

        search_res = patid_search.search(zip_file)
        if not search_res:
            print('ERROR: no match found for pattern: ', args.patid_pattern)
            continue
        patid = search_res.group(1)

        folder = 'native' if 'native' in zip_file else 'mni'

        job_id = job_search.search(zip_file).group(1)
        if not args.redo and job_id in job_map:
            print('Already processed job:', job_id)
            continue

        target_dir = os.path.join(args.outdir, patid, folder)
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        # Extract zip to target location
        dl = ZipFile(zip_file)
        dl.extractall(target_dir)
        dl.close()

        if folder == 'mni':
            csv_report = glob(os.path.join(target_dir, 'report*.csv'))[0]
            temp_df = pd.read_csv(csv_report, sep=';')
            patid = patid_search.search(csv_report).group(1)
            temp_df['Subject'] = patid.split('_')[0]
            temp_df['Patient ID'] = patid

            if any(col for col in temp_df.columns if 'Brainstem' in col):
                report_type = 'subcortical'
            elif any(col for col in temp_df.columns if 'Crus' in col):
                report_type = 'ceres'
            else:
                report_type = 'hips'
                contrast = 'multicontrast' if glob(os.path.join(target_dir, '*t2.nii')) else 'monocontrast'
                method = 'winterburn' if glob(os.path.join(target_dir, '*winterburn*.nii')) else 'kulaga'
                report_type = '_'.join([report_type, method, contrast])

            job_map[job_id] = report_type

            temp_df['type'] = report_type
            temp_df['job_id'] = job_id
            if report_type not in report_dfs.keys():
                report_dfs[report_type] = []
            report_dfs[report_type].append(temp_df)

        dest = os.path.join(args.outdir, patid, job_map[job_id], folder)
        if os.path.exists(dest):
            shutil.rmtree(dest)
            continue
        os.makedirs(dest)

        # move extracted files under pipeline folder, and if args.rename,m replace job name with patid
        for f in glob(os.path.join(target_dir, '*')):
            filename = f.replace(job_search.search(f).group(1), patid) if args.rename and job_search.search(f) else f
            os.rename(f, os.path.join(dest, os.path.basename(filename)))

        os.rmdir(target_dir)

    master_df_list = []
    for type, df_list in report_dfs.items():
        df = pd.concat(df_list).set_index(['Subject', 'Patient ID', 'Age', 'Sex', 'type']).sort_index()
        outfile = os.path.join(args.outdir, type + '_report.csv')
        if args.redo and os.path.exists(outfile):
            old_df = pd.read_csv(outfile).set_index(['Subject', 'Patient ID', 'Age', 'Sex', 'type']).sort_index()
            df = pd.concat([old_df, df])
            df = df[~df.index.duplicated(keep='last')] # drop duplicate indexes + take most recently appended
        df.to_csv(outfile)
        master_df_list.append(df)


    if not master_df_list:
        print('Report files are up to date')
        return

    df = pd.concat(master_df_list)
    # if args.redo:
    #     df = df[~df.index.duplicated(keep='last')] # drop duplicate indexes + take most recently appended
    df = df.unstack().sort_index()
    df.columns = [ ' - '.join(map(str, col)) for col in df.columns ]
    df = df.dropna(how='all', axis=1)
    df.to_csv(os.path.join(args.outdir, 'volbrain_master_report.csv'))

    if args.remove_zips:
        for zip in zips:
            os.remove(zip)

@Gooey
def parse_args():
    parser = GooeyParser()
    parser.add_argument('indir', widget='DirChooser', help='directory containing zips from volbrain')
    parser.add_argument('outdir', widget='DirChooser', help='top-level directory to extract to')
    parser.add_argument('--rename', action='store_true', help='rename extracted files to have patid, not job id')
    parser.add_argument('--remove_zips', action='store_true', help='remove zipfiles from indir (default=keep')
    parser.add_argument('--redo', action='store_true', help='re-extract values from previously processed zips in indir')
    parser.add_argument('--patid_pattern', default='[A-Z]+\d+_s\d', help='regex to extract patid from filename')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    extract_and_combine(args.indir, args.outdir, args.patid_pattern, args.rename, args.redo, args.remove_zips)
