#!/usr/bin/env python
import argparse
import pandas as pd
from sys import stdout
from csv import writer

if __name__ == '__main__':
    parser = argparse.ArgumentParser("Find blank columns")
    parser.add_argument('infile', type=argparse.FileType('r'))
    args = parser.parse_args()
    df = pd.read_csv(args.infile, low_memory=False)
    rowcount = len(df)
    csvout = writer(stdout, delimiter=",")
    csvout.writerow(['name', 'nullpct', 'nullcount', 'uniqcount'])

    for col in df.columns:
        vals = df[col]
        nullct = vals.isnull().value_counts().get(True) or 0
        csvout.writerow([
                col,
                round(nullct * 100.0/rowcount, 1),
                nullct,
                len(vals.unique()),
            ])

