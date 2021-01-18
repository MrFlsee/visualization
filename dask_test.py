import dask.dataframe as dd

df = dd.read_csv('data.csv',dtype={'Active.Mean': 'float64','Active.Std': 'float64','Bwd.IAT.Max': 'float64','Flow.IAT.Max': 'float64','Fwd.IAT.Max': 'float64','Fwd.IAT.Min': 'float64','Fwd.IAT.Total': 'float64','Idle.Mean': 'float64','Idle.Std': 'float64'})
df.head(5)

bdf = dd.read_csv('311.csv',dtype='object')
bdf.head(5)

bdf.groupby(['Agency', 'Location Type']).size().compute()

bdf[['Agency', 'Descriptor']].head(3)

df = bdf.groupby(['Agency']).sum()

df.head(10)