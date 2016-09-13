#!/usr/bin/env python
import argparse
from collections import OrderedDict
from copy import deepcopy
from math import ceil
import pandas as pd
import rtyaml
from sys import stdout

# from sys import stdout


# check nulls df.isnull().groupby('occurrence_date').size()

# check uniques df.groupby('occurrence_date').size()


def get_schema(df):
    """
    Returns fieldnames and likely formats
    {
        name: id
        type: integer
    }

    Modifies df to turn object into string

    """
    fields = []
    for n, dt in enumerate(df.dtypes):
        f = OrderedDict()
        dtype = str(dt)
        f['name'] = df.dtypes.index[n]
        if 'datetime' in f['name']:
            f['type'] = 'datetime'
        elif 'date' in f['name']:
            f['type'] = 'date'
        elif 'int' in dtype:
            f['type'] = 'integer'
        elif 'float' in dtype:
            f['type'] = 'float'
        elif 'bool' in dtype:
            f['type'] = 'boolean'
        else:
            f['type'] = 'string'
            df[f['name']] = df[f['name']].astype(str)
        fields.append(f)
    return fields



def get_nulls(df, schema):
    # returns a new schema
    newschema = deepcopy(schema)
    for field in newschema:
        if df[field['name']].isnull().values.any():
            field['has_nulls'] = True
    return newschema


def get_lengths(df, schema):
    newschema = deepcopy(schema)
    for field in newschema:
        if field['type'] == 'string':
            col = df[field['name']]
            lengths = col[col.notnull()].str.len()
            field['length'] = int(lengths.max())

        elif field['type'] == 'integer':
            col = df[field['name']]
            vals = col[col.notnull()].dropna()
            maxlen = ceil(int(vals.max()) ** (1/8))
            if int(vals.min()) < 0:
                field['unsigned'] = False
                maxlen += 1
            field['length'] = maxlen

        elif field['type'] == 'float':
            col = df[field['name']]
            strvals = col[col.notnull()].dropna().astype(str).str.extract('(-?)(\d+)\.(\d+)', expand=True)
            if bool((strvals[0] == '-').any()):
                field['unsigned'] = False

            try:
                nx = int(strvals[1].str.len().max())
                ny = int(strvals[2].str.len().max())
            except ValueError as err:
                pass
            else:
                field['length'] = [nx+ny, ny]


    return newschema



def get_examples(df, schema):
    newschema = deepcopy(schema)
    for field in newschema:
        col = df[field['name']]
        valuects = col[col.notnull()].value_counts()
        values = [v.item() if hasattr(v, 'item') else v for v in valuects.index.tolist()]
        if len(values) < 10:
            field['enumerations'] = sorted(values)
        else:
            if field['type'] == 'string':
                svals = sorted(values, key=lambda x: len(x))
                _m = len(svals) // 2
                field['examples'] = svals[0:2] + svals[_m:_m+1] + svals[-2:]
            elif field['type'] in ['integer', 'float', 'date', 'datetime']:
                field['examples'] = [min(values), max(values), values[len(values) // 2]]
    return newschema



if __name__ == '__main__':
    parser = argparse.ArgumentParser("Generate schema")
    parser.add_argument('infile', type=argparse.FileType('r'))
    args = parser.parse_args()
    df = pd.read_csv(args.infile)
    schema = get_schema(df)
    schema = get_nulls(df, schema)
    schema = get_lengths(df, schema)
    schema = get_examples(df, schema)
    stdout.write(rtyaml.dump(schema))

