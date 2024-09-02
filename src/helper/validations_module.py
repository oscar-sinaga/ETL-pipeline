import pandas as pd

## Cek Shape data
def check_shape_data(df,nama_tabel):
    print("CHECKING SHAPE DATA")
    rows_amount = df.shape[0]
    columns_amount = df.shape[1]

    print(f'Data {nama_tabel} has {rows_amount} rows and {columns_amount} columns\n')

## Cek tipe data
def check_type_data(df, table_name):
    print("CHECKING DATA TYPE")
    print(f'Each column in {table_name} has the following data types:\n')
    for column in df.columns:
        print(f'Column `{column}` has data type {df[column].dtype}')
    print()

## Check missing value
def check_missing_values(df, table_name):
    print("CHECKING MISSING DATA")
    print(f'Each column in `{table_name}` has the following number of missing values:\n')
    for column in df.columns:
        missing_values = df[column].isnull().sum()
        percentage_missing_values = round(missing_values/len(df)*100,1)
        print(f'Column `{column}` has {missing_values} or {percentage_missing_values}% missing values')
    print()


## Check duplicate
def check_duplicates(df, table_name):
    print("CHECKING DATA DUPLICATES")
    duplicate_count = len(df[df.duplicated()])
    print(f"`{table_name}` has {duplicate_count} duplicate entries \n")

## Check unique values
def check_unique_values(df,table_name):
    print("CHECKING UNIQUE VALUES")
    print(f'Each column in `{table_name}` has the following number of missing values:\n')
    for column in df.columns:
        unique_values = df[column].unique()
        if len(unique_values)<=100:
            print(f'''Jumlah unique values pada kolom `{column} `adalah sebanyak : {len(unique_values)} yaitu : \n {unique_values}\n''')
        else:
            print(f'''Jumlah unique values pada kolom `{column}` adalah sebanyak : {len(unique_values)}\n''')


def validation_process(df, table_name):
    #Calculate Percentage of Missing Value
    print(f"========== Start {table_name} Pipeline Validation ==========")
    # check data shape
    check_shape_data(df,table_name)

    ## Cek tipe data
    check_type_data(df, table_name)

    ## Check missing value
    check_missing_values(df, table_name)
    
    ## Check duplicate
    check_duplicates(df, table_name)

    ## Check unique values
    check_unique_values(df,table_name)
    print("========== End Pipeline Validation ==========")