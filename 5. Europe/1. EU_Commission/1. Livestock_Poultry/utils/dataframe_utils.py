import pandas

def format_dates(df: pandas.DataFrame, cols=('WEEK_START_DATE', 'WEEK_ENDING_DATE')) -> pandas.DataFrame:
    for col in cols:
        df[col] = pandas.to_datetime(df[col]).dt.strftime('%Y-%m-%d')
    return df