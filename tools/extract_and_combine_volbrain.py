import argparse
import os
import pandas as pd
import re
import shutil

from glob import glob
from gooey import Gooey, GooeyParser
from zipfile import ZipFile

def get_processed_jobs(outdir):
    job_map = {}
    reports = glob(os.path.join(outdir, '*_report.csv'))
    for report in reports:
        df = pd.read_csv(report)
        job_map.update(dict(zip(df['job_id'], df['type'])))
    return job_map


def extract_and_combine(indir, outdir, patid_pattern, rename=False, redo=False):
    # extract/rename files in volbrain zips
    job_search = re.compile(r'(job\d{6})')

    patid_search = re.compile('({})'.format(patid_pattern), flags=re.IGNORECASE)

    zips = glob(os.path.join(indir, '*.zip'))

    native_zips = [ f for f in zips if 'native' in f ]
    mni_zips = [ f for f in zips if f not in native_zips ]

    # create map of job id to report type in case mni has already been processed but native has not
    job_map = get_processed_jobs(outdir)

    report_dfs = {}
    for zip_file in mni_zips + native_zips: # make sure we're always iterating over the mni ones first (need lookup table for report type)
        search_res = patid_search.search(zip_file)
        if not search_res:
            print('ERROR: no match found for pattern: ', patid_pattern)
            continue
        patid = search_res.group(1)

        folder = 'native' if 'native' in zip_file else 'mni'

        job_id = job_search.search(zip_file).group(1)

        # determine if zip file has already been processed
        if not redo and job_id in job_map:
            dest = os.path.join(outdir, job_map[job_id], patid, folder)
            if os.path.exists(os.path.join(outdir, job_map[job_id], patid, folder)): # lookup dest by job id (must already be in map)
                print('Already processed:', job_id, patid, folder)
                continue

        print('Processing ', zip_file)

        target_dir = os.path.join(outdir, 'temp', patid, folder)
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

        dest = os.path.join(outdir, job_map[job_id], patid, folder)
        if os.path.exists(dest):
            shutil.rmtree(target_dir)
            continue
        os.makedirs(dest)

        # move extracted files under pipeline folder, and if rename,m replace job name with patid
        for f in glob(os.path.join(target_dir, '*')):
            filename = f.replace(job_search.search(f).group(1), patid) if rename and job_search.search(f) else f
            os.rename(f, os.path.join(dest, os.path.basename(filename)))

        os.rmdir(target_dir)

    # if no new reports, exit
    if not any(report_dfs.values()):
        print('Report files are up to date')
        return

    # otherwise, combine old rows with new rows and save separate reports for each type
    for type, df_list in report_dfs.items():
        report_frames = []

        # include existing rows if not redo-ing
        outfile = os.path.join(outdir, type + '_report.csv')
        if not redo and os.path.exists(outfile):
            report_frames.append(pd.read_csv(outfile))

        report_frames += df_list
        df = pd.concat(report_frames, sort=False).set_index(['Subject', 'Patient ID', 'Age', 'Sex', 'type'])
        df = df[~df.index.duplicated(keep='last')] # drop duplicate indexes + take most recently appended
        df = df.sort_index()
        df.to_csv(outfile)


@Gooey
def parse_args():
    parser = GooeyParser()
    parser.add_argument('indir', widget='DirChooser', help='directory containing zips from volbrain')
    parser.add_argument('outdir', widget='DirChooser', help='top-level directory to extract to')
    parser.add_argument('--rename', action='store_true', help='rename extracted files to have patid, not job id')
    parser.add_argument('--redo', action='store_true', help='re-extract values from previously processed zips in indir')
    parser.add_argument('--patid_pattern', default=r'[A-Z]+\d+_s\d', help='regex to extract patid from filename')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    extract_and_combine(args.indir, args.outdir, args.patid_pattern, args.rename, args.redo)
