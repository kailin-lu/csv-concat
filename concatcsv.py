# Command line tool to simply Pandas concat CSVs

import os
from pathlib import Path
import argparse
from collections import defaultdict
import pandas as pd

parser = argparse.ArgumentParser(prog='concatcsv',
                                 description='Merge CSVs on header row names')

parser.add_argument('--dir',
                    type=str,
                    default='.',
                    help='directory to look for CSVs files')
parser.add_argument('--outdir',
                    type=str,
                    default='.',
                    help='directory to put merged CSV file')
parser.add_argument('--output_name',
                    type=str,
                    default='merged',
                    help='name of output csv file')
parser.add_argument('--level',
                    type=float,
                    default=1.0,
                    help='min percentage of files needed to contain a column in include in final output')
parser.add_argument('-summarize',
                    action='store_true',
                    help='print common column names and count of files containing each column')

args = parser.parse_args()


def generate_csvs(path, file_list):
    """Pandas load CSV for CSV file in directory path
    """
    for f in file_list:
        yield pd.read_csv(os.path.join(path, f))


def common_columns(csv_gen):
    """Returns a dictionary of column names and count of files with column
    """
    cols = defaultdict(int)
    for df in csv_gen:
        for col in df.columns:
            cols[col] += 1
    return cols


def summarize_columns(cols_dict, nfiles):
    """Returns a DataFrame of column and file count
    """
    print(f'TOTAL COUNT OF CSV FILES: {nfiles}')
    summary = pd.DataFrame.from_dict(cols_dict, orient='index', columns=['COUNT_FILES'])
    summary['PCT_CONTAINS_COL'] = summary['COUNT_FILES'].apply(lambda x: round(x / nfiles, 2))
    return summary


def custom_concat(summary, path, file_list):
    """Concats list of CSVs with selected popularity of columns
    """
    print(f'Min percent needed to be included: {args.level * 100}%')
    gen = generate_csvs(path, file_list)
    include_cols = summary.index.values[summary['PCT_CONTAINS_COL'] >= args.level]
    if args.level == 1.0:
        print('(column intersection)\n')
        output = pd.concat([f for f in gen], axis=0, ignore_index=True)
    elif args.level == 0.0:
        print('(column union)\n')
        output = pd.concat([f for f in gen], axis=0, ignore_index=True)
    else:
        output = pd.concat([f for f in gen], axis=0, ignore_index=True)
    print('\nOUTPUT CSV\n')
    print(output.info())
    return output


if __name__ == '__main__':
    # Check input and output directories
    input_dir = Path(args.dir)
    output_dir = Path(args.outdir)
    assert os.path.isdir(input_dir), "Input path is not a directory"
    assert os.path.isdir(output_dir), "Output dir is not a directory"

    files = os.listdir(input_dir)

    # Get list of CSV files
    csv_files = [f for f in files if f.split('.')[-1] == 'csv']
    total_files = len(csv_files)
    if total_files == 0:
        print('No CSVs to concatenate, exiting.')
        exit()

    csv_gen = generate_csvs(input_dir, csv_files)

    # Calculate common columns across files
    common_cols = common_columns(csv_gen)

    summary = summarize_columns(common_cols, total_files)
    if args.summarize:
        print(summary)

    output = custom_concat(summary, input_dir, csv_files)
    output.to_csv(os.path.join(output_dir, args.output_name +'.csv'), index=False)
    output_path = os.path.abspath(args.outdir)
    print(f'Combined CSV saved to {output_path}')
