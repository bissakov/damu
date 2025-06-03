import numpy as np
import pandas as pd
from utils.utils import days360

# DO NOT EDIT, DO NOT ADD CODE THAT IS NOT RELATED TO FORMULAS THEMSELVES
# ONLY NEW FORMULAS
# FOLLOW THE FORMAT: {formula_name}_{index}


def calc_subsidy_sum1(original: pd.DataFrame) -> pd.DataFrame:
    # df: pd.DataFrame = original.copy()
    # df["subsidy_sum"] = (
    #     (
    #         df["principal_debt_balance"].shift(1)
    #         * (df["rate"] / df["day_year_count"])
    #         * df["day_count"]
    #     )
    #     .round(2)
    #     .astype("Int64")
    # )

    df: pd.DataFrame = original.copy()
    # df["rate"] = (df["rate"] * 100).round().astype("Int64")

    num = df["principal_debt_balance"].shift(1) * df["rate"] * df["day_count"]
    den = df["day_year_count"] * 10_000
    df["subsidy_sum"] = ((num + (den // 2)) // den).astype("Int64")
    return df


def calc_subsidy_sum2(original: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = original.copy()

    num = df["principal_debt_balance"] * df["rate"] * df["day_count"]
    den = df["day_year_count"] * 10_000
    df["subsidy_sum"] = ((num + (den // 2)) // den).astype("Int64")

    # df["subsidy_sum"] = (
    #     (df["principal_debt_balance"] * (df["rate"] / df["day_year_count"]) * df["day_count"])
    #     .round(2)
    #     .astype("Int64")
    # )
    return df


def calc_principal_balance_check1(original: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = original.copy()
    df["principal_balance_check"] = (
        (
            df["principal_debt_balance"].shift(1)
            - df["principal_debt_repayment_amount"]
        ).fillna(0)
    ) == df["principal_debt_balance"]
    return df


def calc_principal_balance_check2(original: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = original.copy()
    df["principal_balance_check"] = (
        df["principal_debt_balance"]
        .shift()
        .fillna(0)
        .sub(df["principal_debt_repayment_amount"].shift().fillna(0))
        == df["principal_debt_balance"]
    )
    return df


def calc_principal_balance_check3(original: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = original.copy()
    df["principal_balance_check"] = np.where(
        df.index == df.index[-1],
        (
            df["principal_debt_balance"] - df["principal_debt_repayment_amount"]
            == 0
        ),
        (
            df["principal_debt_balance"] - df["principal_debt_repayment_amount"]
            == df["principal_debt_balance"].shift(-1)
        ),
    )
    return df


def calc_principal_balance_check4(original: pd.DataFrame) -> pd.DataFrame:
    df: pd.DataFrame = original.copy()
    df["principal_balance_check"] = (
        df["principal_debt_balance"] - df["principal_debt_repayment_amount"]
    ) == df["principal_debt_balance"].shift(-1)
    return df


def calc_day_count1(debt_repayment_dates: pd.Series) -> pd.Series:
    days = debt_repayment_dates.diff().dt.days
    days = days.where(~days.between(25, 35), 30)
    days = days.astype("Int64")
    return days

    # result = debt_repayment_dates.diff().dt.days.astype("Int64")
    # result = pd.Series(np.where((result - 30).abs() < 5, 30.0, result))
    # return result


def calc_day_count2(debt_repayment_dates: pd.Series) -> pd.Series:
    df = pd.DataFrame(
        {
            "debt_repayment_date": debt_repayment_dates,
            "prev_date": debt_repayment_dates.shift(),
        }
    )
    day_count = df.apply(
        lambda row: days360(
            row["prev_date"].date(), row["debt_repayment_date"].date()
        )
        if pd.notnull(row["prev_date"])
        else None,
        axis=1,
    )
    return day_count.astype("Int64")


def calc_day_count3(debt_repayment_dates: pd.Series) -> pd.Series:
    return debt_repayment_dates.diff().dt.days.astype("Int64")
