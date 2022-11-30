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
    
    # europe_df = pandas.read_csv(f"{folder}\europe_population.csv")
    # na_df = pandas.read_csv(f"{folder}\\na_population.csv")
    us_df = pandas.read_csv(f"{folder}\counties_population.csv").sort_values(by="Combined Key", ignore_index=True)
    counties_list = us_df["Combined Key"].to_list()

    prev_month_df_us = pandas.read_csv(f"{folder}\dec2020.csv")[["Combined_Key", "Confirmed"]]
    # prev_month_df_us = prev_month_df_us.loc[prev_month_df_us["Country_Region"] == "US"].drop(columns=["Country_Region"])
    prev_month_df_us = prev_month_df_us[prev_month_df_us["Combined_Key"].isin(counties_list)]

    prev_month_df_us = prev_month_df_us.sort_values(by="Combined_Key", ignore_index=True)
    print(prev_month_df_us.dtypes)
    print(prev_month_df_us["Combined_Key"])

    # print(prev_month_df_us)

    for month in months:
        # month_df_europe = pandas.read_csv(f"{folder}\{month}.csv")[["Country_Region", "Confirmed"]]
        # month_df_na = pandas.read_csv(f"{folder}\{month}.csv")[["Province_State", "Confirmed"]]
        month_df_us = pandas.read_csv(f"{folder}\{month}.csv")[["Combined_Key", "Confirmed"]]

        # month_df_europe = month_df_europe[month_df_europe["Country_Region"].isin(europe_df["Region"])].groupby(month_df_europe["Country_Region"], as_index=False).aggregate({"Country_Region": "first", "Confirmed": "sum"})
        # month_df_na = month_df_na[month_df_na["Province_State"].isin(na_df["Region"])].groupby(month_df_na["Province_State"], as_index=False).aggregate({"Province_State": "first", "Confirmed": "sum"})
        # month_df_us = month_df_us.loc[month_df_us["Country_Region"] == "US"].drop(columns=["Country_Region"])
        month_df_us = month_df_us[month_df_us["Combined_Key"].isin(counties_list)]
        month_df_us =  month_df_us.sort_values(by="Combined_Key", ignore_index=True)
        if month == "jan": print(month_df_us["Combined_Key"])

        # europe_df = europe_df.merge(month_df_europe, left_on="Region", right_on="Country_Region").drop("Country_Region", axis=1).rename(columns={"Confirmed": f"{month}_cases"})
        # na_df = na_df.merge(month_df_na, left_on="Region", right_on="Province_State").drop("Province_State", axis=1).rename(columns={"Confirmed": f"{month}_cases"})
        us_df = us_df.merge(month_df_us, left_on="Combined Key", right_on="Combined_Key").drop("Combined_Key", axis=1).rename(columns={"Confirmed": f"{month}_cases"})
        if month == "jan": print(us_df[f"{month}_cases"])
        if month == "jan": print(prev_month_df_us["Confirmed"])
        us_df[f"{month}_cases"] = us_df[f"{month}_cases"] - prev_month_df_us["Confirmed"]
        if month == "jan": print(us_df)

        # europe_df[f"{month}_rate"] = europe_df[f"{month}_cases"]/europe_df["Population"]
        # na_df[f"{month}_rate"] = na_df[f"{month}_cases"]/na_df["Population"]
        us_df[f"{month}_rate"] = us_df[f"{month}_cases"]/us_df["Population"]

        prev_month_df_us = month_df_us.copy(deep=True)

    # europe_df["total"] = europe_df[[mon + "_cases" for mon in months]].sum(axis=1)
    # na_df["total"] = na_df[[mon + "_cases" for mon in months]].sum(axis=1)
    us_df["total"] = us_df[[mon + "_cases" for mon in months]].sum(axis=1)
    us_df["total rate"] = us_df["total"] / us_df["Population"]

    # europe_df.to_csv(f"{os.getcwd()}\output\europe.csv")
    # na_df.to_csv(f"{os.getcwd()}\output\\na.csv")
    us_df.to_csv(os.getcwd() + r"\output\us.csv")

    # europe_df["strata"] = "Europe"
    # na_df["strata"] = "North America"

    # total_df = pandas.concat([europe_df, na_df])
    # total_df = total_df[total_df.columns.to_list()[-1:] + total_df.columns.to_list()[:-1]]
    # total_df.to_csv(f"{os.getcwd()}\output\\total.csv")


def main():
    
    data = get_data()


main()