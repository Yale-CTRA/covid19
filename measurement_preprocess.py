import pandas as pd
from Projects.covid19.reader import read_data

data_dict = read_data() # dictionary of pandas DFs w/ keys as folder names

def create_preview(name):
    return data_dict[name].iloc[:1000,:]
previews = {name: create_preview(name) for name in data_dict.keys()}


###############################################################################
######### MEASUREMENT
name = 'measurement'
preview = previews[name]
data = data_dict[name]
data.set_index(['person_id', 'visit_occurrence_id', 
                'measurement_datetime', 'measurement_id'], inplace = True)
data.sort_index(inplace = True)

def multiindex_pivot(df, columns=None, values=None):
    # modified from:
    # https://github.com/pandas-dev/pandas/issues/23955
    # since pandas wont let you pivot a multi-indexed dataframe
    names = list(df.index.names)
    tuples_index = df.index.values
    df.reset_index(drop = True, inplace = True)
    df = df.assign(tuples_index=tuples_index)
    df = df.pivot(index="tuples_index", columns=columns, values=values)
    tuples_index = df.index  # reduced
    index = pd.MultiIndex.from_tuples(tuples_index, names=names)
    df.index = index
    return df

# exploration func
def matcher(series, query):
    return series.loc[[query in item.lower() for item in series.index]]

# filter
m_counts = data['measurement_source_value'].value_counts()
m_count_subset = m_counts[m_counts >= 1000]
keep_rows = data['measurement_source_value'].apply(lambda x: x in m_count_subset)
keep_cols = ['value_as_number', 'measurement_source_value', 'value_source_value']
data = data.loc[keep_rows,:]

# make sparse dataset
numeric_rows = data['value_as_number'].isnull()

data = multiindex_pivot(data, columns='measurement_source_value', 
                   values='value_source_value')
data.reset_index(level = 3, drop = True, inplace = True)