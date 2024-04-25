#%%

LOAD_445 = True
LOAD_SLS = True
LOAD_LOGILITY_CALENDER = True
LOAD_DATE_DIM = False

import pandas as pd
import pendulum as pnd
from hbb_da_zen.db_lib import sql_utils as su



path_445 = r'\\HB-DEV-PWRBI\e\temp\calendar_445_apr2024.xlsx'



#%%

def pd2bi(df, schema, table):
    ''' writes df to to HBBDWSQ1.HBBDWBI '''

    # write to HBBDWBI
    engine = su.sql_engine('HBBDWSQL1','HBBDWBI')

    df.to_sql(table, engine, 
                    schema=schema,
                    if_exists='append',
                    index=False
    )


#%%

def load_445(path):
    ''' Loads 445 calendar from path.
        Changes every year; logic is unclear.
        Get from COPS (Ken C. or Bennie Smith)
    '''

    #read excel
    df = pd.read_excel(path, sheet_name='analytics')

    #delete current table:
    su.query_pbi('drop table enterprise.calendar_445_weeks')

    #load df into table
    pd2bi(df, 'enterprise','calendar_445_weeks')


def load_log_cal():
    ''' Loads Logility calendar from HB-LOG-PROD.LVSPROD'''

    df = su.select_from_db('select * from dbo.SCP_CAL_YEAR', 
                           'HB-LOG-PROD', 
                           'LVSPROD'
        )

    # fix WK52_END_DATE where equals '2023-01-01', should equal '2022-12-25'
    df.loc[df.WK52_END_DATE == '2023-01-01', 'WK52_END_DATE'] = '2022-12-25'

    # drop columns with "PROD"/"QTR" in name
    df = df.loc[:,~df.columns.str.contains('PROD')]
    df = df.loc[:,~df.columns.str.contains('PRD')]
    df = df.loc[:,~df.columns.str.contains('QTR')]

    df.drop(columns = ['DRP_DAYS_YR_NBR','WKS_YR_NBR'],
            inplace=True)


    #make a list of all columns that contain "WK" in the name
    wk_cols = [col for col in df.columns if 'WK' in col]
    assert len(wk_cols) == 53, 'wk_cols should have 53 columns'

    
    # if week 52 == week 53, replace w/ NaN
    df.loc[df['WK53_END_DATE'] == df['WK52_END_DATE'], 'WK53_END_DATE'] = None

    # if all rows in wk53 is null, drop columns:
    if df.WK53_END_DATE.isnull().all():
        df = df.drop(columns=['WK53_END_DATE'])

    df.columns = df.columns.str.lower()

    df.rename(columns = {'cal_yr': 'year','cal_bgn_date': 'year_begin'},
                            inplace=True)

    #wk_cols list to lowercase
    wk_cols = [col.lower() for col in wk_cols]

    # melt columns into rows
    dfm = df.melt(id_vars='year', 
                 value_vars=wk_cols, 
                 var_name='week', 
                 value_name='date') \
            .sort_values(['year','week'])

    # extract week number from column name
    dfm.week = dfm.week.str.extract('(\d+)').astype(int)

    # get rid of 53rd week where NaT
    dfm.dropna(inplace=True)

    # subtract 6 days from each date to get week start date
    dfm['week_start'] = dfm.date - pd.Timedelta(days=6)

    #rename week to end of week
    dfm = dfm.rename(columns = {'date':'week_end'})

    dfm = dfm[['year','week','week_start','week_end']]

    #add "_log" to each column name
    dfm.columns = [col + '_log' for col in dfm.columns]

    #delete current table:
    su.query_pbi('drop table enterprise.calendar_logility')

    #load df into table
    pd2bi(dfm, 'enterprise','calendar_logility')    
    


def load_date_dim():
    ''' Loads date_dim from HBBDWSQL1.HBBDWBI '''

    df2 = su.select_from_db(f'select * from enterprise.date_dim', 'HBBDWSQL1', 'HBBDWBI')
    df2.date = pd.to_datetime(df2.date)










if LOAD_445:
    load_445(path_445)





#%%

# e = '''select distinct
#             month_name,
#             min(monday_begin) OVER
#                 (PARTITION BY month_name, year) as monday_begin_min
#             --max(monday_begin) OVER
#             --	(PARTITION BY month_name, year) as monday_begin_max
#             --max(sunday_end)
#         from cops.month_calendar
#         where month_name is not null
#         order by monday_begin_min'''


# mo_cal = su.select_from_db(e, 'HBBDWSQL1', 'HBBDWBI')
# mo_cal.columns = ['month', 'month_begin']
# mo_cal.month_begin = pd.to_datetime(mo_cal.month_begin)




#%%
# e2 = '''select date from cops.date_dim
#         where date >= '2022-01-01' '''

# d_cal = su.select_from_db(e2, 'HBBDWSQL1', 'HBBDWBI')
# d_cal.date = pd.to_datetime(d_cal.date)

# d_cal = d_cal[2:]

# # get last row where date is and cut off

# mo = d_cal.merge(mo_cal, how='left', left_on='date', right_on = 'month_begin')

# ind = mo[mo.month.notnull()].tail(1).index.astype(int)[0]
# mo = mo[:ind+1]

# mo = mo.fillna(method='ffill').drop(columns = 'month_begin')