import py_vollib.black_scholes_merton.implied_volatility
import py_vollib_vectorized
from datetime import datetime, date, time as time_dt
import pandas as pd
import numpy as np

# Constants
r = 0.05
q = 0.05
DAYS_IN_YEAR = 252
TRADING_MIN_IN_DAY = 375


def calculate_expiry_time(row, ref_date=None):
    """
    Function to calculate the time to expiry for an option
    Takes in a row of an option chain DataFrame
    """
    if ref_date is None:
        ref_date = datetime.now().date()

    if isinstance(ref_date, str):
        # assume the date is in the format "dd-mmm-yyyy" as trhat's the format in breeze
        ref_date = datetime.strptime(ref_date, "%d-%b-%Y").date()

    return (
        datetime.strptime(row["expiry_date"], "%d-%b-%Y").date() - ref_date
    ).days / 252


def calclate_iv_greeks(
    df: pd.DataFrame,
    delta=False,
    gamma=False,
    rho=False,
    theta=False,
    vega=False,
):
    """
    Function to calculate implied volatility for an option chain
    Data Frame must have columns:
    - spot_price, best_bid_price, best_offer_price, strike_price, right, expiry_date
    """
    df["mid_price"] = (df["best_bid_price"] + df["best_offer_price"]) / 2
    df["t"] = df.apply(lambda row: calculate_expiry_time(row), axis=1)
    df["flag"] = df["right"].apply(lambda x: "c" if x == "Call" else "p")

    fun_args = {
        "price": df["mid_price"],
        "S": df["spot_price"],
        "K": df["strike_price"],
        "t": df["t"],
        "r": r,
        "flag": df["flag"],
        "q": q,
        "on_error": "warn",
        "model": "black_scholes_merton",
        "return_as": "numpy",
    }

    df["iv"] = py_vollib_vectorized.vectorized_implied_volatility(**fun_args)
    if any([delta, gamma, rho, theta, vega]):
        del fun_args["price"]
        fun_args["sigma"] = df["iv"]
    if delta:
        df["delta"] = py_vollib_vectorized.greeks.delta(**fun_args)
    if gamma:
        df["gamma"] = py_vollib_vectorized.greeks.gamma(**fun_args)
    if rho:
        df["rho"] = py_vollib_vectorized.greeks.rho(**fun_args)
    if theta:
        df["theta"] = py_vollib_vectorized.greeks.theta(**fun_args)
    if vega:
        df["vega"] = py_vollib_vectorized.greeks.vega(**fun_args)

    return df


def calculate_rv(df: pd.DataFrame, window=30):
    """
    Function to calculate realised volatility based on minute level tick
    """
    # Calculate log returns
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))

    # Calculate realized volatility for the last 30 minutes
    # guard for the fact that data frame may have less than 30 rows
    # in that case use the lower number of rows
    window = min(window, len(df.index))
    df["rv"] = df["log_return"].rolling(window=window).std() * np.sqrt(
        TRADING_MIN_IN_DAY * DAYS_IN_YEAR
    )  # Annualization factor

    return df


def get_ATM_data(df, ref_price=None, in_money=True):
    """
    Filters a DataFrame containing option data to ATM options.

    Args:
        df (pd.DataFrame): DataFrame with columns "expiry_date", "strike_price",
        "spot_price", "right", "quote_time".
        ref_price (float, optional): Reference price to use. If None, "spot_price" will be used.
        Defaults to None.
        in_money (bool, optional): Whether to filter for in-the-money options (True) or
        out-of-the-money options (False). Defaults to True.

    Returns:
        pd.DataFrame: Filtered DataFrame containing the selected rows.
    """

    # Copy spot_price or use ref_price
    df["ref_price"] = df["spot_price"] if ref_price is None else ref_price

    df["moneyness"] = np.where(
        df["right"] == "Call",
        df["ref_price"] - df["strike_price"],
        df["strike_price"] - df["ref_price"],
    )

    # Filter rows based on moneyness and right (Call/Put)
    if in_money:
        filtered_df = df[df["moneyness"] >= 0]
    else:
        filtered_df = df[df["moneyness"] <= 0]

    indexes = []

    for quote_time in filtered_df["quote_time"].unique():
        for expiry_date in filtered_df["expiry_date"].unique():
            expiry_group = filtered_df[
                (filtered_df["expiry_date"] == expiry_date)
                & (filtered_df["quote_time"] == quote_time)
            ]

            # the df has only -ve or +ve values at this point so we can take abs
            # and find the miz +ve value for Call and Put both
            if in_money:
                c_idx = expiry_group[expiry_group["right"] == "Call"][
                    "moneyness"
                ].idxmin()
                p_idx = expiry_group[expiry_group["right"] == "Put"][
                    "moneyness"
                ].idxmin()
            else:
                c_idx = expiry_group[expiry_group["right"] == "Call"][
                    "moneyness"
                ].idxmax()
                p_idx = expiry_group[expiry_group["right"] == "Put"][
                    "moneyness"
                ].idxmax()
            indexes += [c_idx, p_idx]

    atm_df = filtered_df.loc[indexes]
    return atm_df
