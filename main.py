from re import A
import requests, pandas, os

# population data:
# Europe: United Nations, Department of Economic and Social Affairs, Population Division (2022). World Population Prospects: The 2022 Revision, custom data acquired via website
# US counties: https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-total.html
# US states: https://www2.census.gov/programs-surveys/popest/datasets/2020-2021/state/totals/
# canada provinces: https://www150.statcan.gc.ca/n1/daily-quotidien/220209/t001a-eng.htm


def get_data():
    folder = f"{os.getcwd()}\data"
    months = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    
    us_df = pandas.read_csv(f"{folder}\counties_population.csv").sort_values(by="Combined Key", ignore_index=True)
    counties_list = us_df["Combined Key"].to_list()

    prev_month_df_us = pandas.read_csv(f"{folder}\dec2020.csv")[["Combined_Key", "Confirmed"]]
    prev_month_df_us = prev_month_df_us[prev_month_df_us["Combined_Key"].isin(counties_list)]

    prev_month_df_us = prev_month_df_us.sort_values(by="Combined_Key", ignore_index=True)

    for month in months:
        month_df_us = pandas.read_csv(f"{folder}\{month}.csv")[["Combined_Key", "Confirmed"]]

        month_df_us = month_df_us[month_df_us["Combined_Key"].isin(counties_list)]
        month_df_us =  month_df_us.sort_values(by="Combined_Key", ignore_index=True)

        us_df = us_df.merge(month_df_us, left_on="Combined Key", right_on="Combined_Key").drop("Combined_Key", axis=1).rename(columns={"Confirmed": f"{month}_cases"})
        us_df[f"{month}_cases"] = us_df[f"{month}_cases"] - prev_month_df_us["Confirmed"]
        
        us_df[f"{month}_rate"] = us_df[f"{month}_cases"]/us_df["Population"]

        prev_month_df_us = month_df_us.copy(deep=True)


    us_df["total"] = us_df[[mon + "_cases" for mon in months]].sum(axis=1)
    us_df["total rate"] = us_df["total"] / us_df["Population"]


    us_df.to_csv(os.getcwd() + r"\output\us.csv")


def main():
    
    data = get_data()


main()