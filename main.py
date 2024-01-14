import pandas as pd
import numpy as np
import logging
import os


def extract(store_data, extra_data):
    extra_df = pd.read_parquet(extra_data)
    merged_df = store_data.merge(extra_df, on="index")
    return merged_df


merged_df = extract(grocery_sales, "extra_data.parquet")


def transform(raw_data):
    raw_data.fillna(
        {
            'CPI': raw_data['CPI'].mean(),
            'Weekly_Sales': raw_data['Weekly_Sales'].mean(),
            'Unemployment': raw_data['Unemployment'].mean(),
        }, inplace=True
    )

    raw_data["Date"] = pd.to_datetime(raw_data["Date"], format="%Y-%m-%d")
    raw_data["Month"] = raw_data["Date"].dt.month

    raw_data = raw_data.loc[raw_data["Weekly_Sales"] > 10000, :]

    raw_data = raw_data.drop(
        ["index", "Temperature", "Fuel_Price", "MarkDown1", "MarkDown2", "MarkDown3", "MarkDown4", "MarkDown5", "Type",
         "Size", "Date"], axis=1)
    return raw_data


clean_data = transform(merged_df)


def avg_monthly_sales(clean_data):
    holidays_sales = clean_data[["Month", "Weekly_Sales"]]

    holidays_sales = (holidays_sales.groupby("Month")
                      .agg(Avg_Sales=("Weekly_Sales", "mean"))
                      .reset_index().round(2))
    return holidays_sales


agg_data = avg_monthly_sales(clean_data)


def load(full_data, full_data_file_path, agg_data, agg_data_file_path):
    full_data.to_csv(full_data_file_path, index=False)
    agg_data.to_csv(agg_data_file_path, index=False)


load(clean_data, "clean_data.csv", agg_data, "agg_data.csv")


def validation(file_path):
    file_exists = os.path.exists(file_path)
    if not file_exists:
        raise Exception(f"There is no file at the path {file_path}")


validation("clean_data.csv")
validation("agg_data.csv")
