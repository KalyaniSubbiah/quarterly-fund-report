import pandas as pd

def output_rbr_returns(company_data_filepath, company_rbr_filepath, rbr_names, metrics_dict= {'Port. Total Return': 'sum', 'Port. Contribution To Return': 'sum', 'Port. Beginning Weight': 'sum', 'Port. Ending Weight': 'sum'}):
    """
    Arguments:
    company_data_filepath (str): path to file containing company gvkeys, isins and data on metrics 
    company_rbr_filepath (str): path to file containing company gvkeys, isins and data on rbr taxonomies 
    rbr_names (list): list of rbr names (ex. ["Activity", "Thematic", "Resource"]) Strings need to be exact.
    metrics_dict (dict): Dictionary containing metrics as the keys with the values as the operation required
                        for calculation by rbr category (sum, min etc.).
                        Operations allowed: 'sum', 'mean', 'min', 'max'.
                        See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.agg.html
                        for usage.
    Returns:
    pd.DataFrame, writes dataframe to file 
    """
    # load company data file
    company_data = pd.read_csv(company_data_filepath, sep='\t')
    # sum multiple isin contributions for each gvkey
    company_data2 = company_data.groupby(['Gvkey']).agg(metrics_dict)
    company_data2 = company_data2.reset_index()
    # load company rbr file
    company_rbr = pd.read_excel(company_rbr_filepath)
    # change name from GVKey to Gvkey to merge on the gvkey column
    company_rbr = company_rbr.rename(columns={"GVKey": "Gvkey"})
    # remove extra columns from excel
    for x in company_rbr.columns:
        if "Unnamed" in x:
            company_rbr = company_rbr.drop(columns=[x])
    # merge both dataframes
    company = company_data2.merge(company_rbr, on = ['Gvkey'], how = 'outer')
    # filter dataframe to include only gvkeys from the company_data file
    company_filtered = company[company['Gvkey'].isin(company_data['Gvkey'])]

    returns = pd.DataFrame()
    for column_name in company_filtered.columns:
        for rbr_name in rbr_names:
            if column_name[:-1] == rbr_name + " Level ":
                # remove duplicates within the category
                filter_keys = [column_name, 'Gvkey'] + list(metrics_dict.keys())
                df = company_filtered[filter_keys]
                df = df.drop_duplicates()
                # aggregate within rbrs
                sum_df = df.groupby([column_name]).agg(metrics_dict)
                sum_df = pd.DataFrame.reset_index(sum_df, level=None, drop=False, inplace=False, col_level=0, col_fill='')
                sum_df = sum_df.rename(columns={column_name: "RBR category"})
                sum_df["RBR Name"] = rbr_name
                sum_df["RBR Level"] = column_name
                returns = pd.concat([returns, sum_df])
    # rearranging and renaming columns
    cols = returns.columns.tolist()
    cols = cols[-2:] + cols[:-2]
    returns = returns[cols]
    returns = returns.rename(columns={"Gvkey": "Number of Gvkeys"})
    # write to csv
    returns.to_csv("returns.csv")
    return returns
