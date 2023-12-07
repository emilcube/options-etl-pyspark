import numpy as np
from scipy import stats
import datetime as dt
from py_vollib.black_scholes.implied_volatility import implied_volatility

def calc_implied_vol(option_price, spot_price, strike, time_to_expiration, side):
    MAX_ITERATIONS = 200
    PRECISION = 1.0e-5
    sigma = 0.5
    for i in range(0, MAX_ITERATIONS):
        price = price_by_BS(spot_price, strike, time_to_expiration, sigma, side.lower())
        vega = bs_vega(spot_price, strike, time_to_expiration, sigma)
        diff = option_price - price
        if (abs(diff) < PRECISION):
            return sigma
        sigma = sigma + diff / vega  # f(x) / f'(x)
    return sigma


def bs_vega(S, K, T, r, sigma):
    return S * pdf(d1(S, K, T, r, sigma)) * np.sqrt(T)

def bs_rtv(S, K, T, r, sigma):
    return 0.01 * S * pdf(d1(S, K, T, r, sigma))

def price_by_BS(S, K, T, sigma, option_type):
    if option_type == 'c':
        return S * cdf(d1(S, K, T, sigma)) - K * cdf(d2(S, K, T, sigma))
    elif option_type == 'p':
        return K * cdf(-d2(S, K, T, sigma)) - S * cdf(-d1(S, K, T, sigma))


def error_handler(func):
    def inner_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            pass

    return inner_func


# def d1(s, k, t, iv):
#     return (np.log(s / k) + (iv ** 2 / 2) * t) / (iv * np.sqrt(t))


# def d2(s, k, t, iv):
#     return d1(s, k, t, iv) - iv * np.sqrt(t)

def d1(s, k, t, r, v):
    return (np.log(s / k) + (r + (v * v) / 2.0) * t) / (v * np.sqrt(t))

def d2(s, k, t, r, v):
    return d1(s, k, t, r, v) - v * np.sqrt(t)


def cdf(value):
    return stats.norm.cdf(value)


def pdf(value):
    return stats.norm.pdf(value)


def delta(s, k, t, iv, opt_type):
    if opt_type == 'c':
        return cdf(d1(s, k, t, iv))
    else:
        return cdf(d1(s, k, t, iv)) - 1


@error_handler
def calc_iv(row):
    return implied_volatility(row['real_prc'], row['spot'], row['strike'], row['maturity'], 0, row['side'].lower())
    #return calc_implied_vol(row['real_prc'], row['spot'], row['strike'], row['maturity'], row['side'].lower())


@error_handler
def calc_delta(row):
    return delta(row['spot'], row['strike'], row['maturity'], row['iv'], row['side'].lower())


def get_closest_val(items, pivot):
    return min(items, key=lambda x: abs(abs(x) - pivot))


def convert_exp_to_str_date(string):
    return dt.datetime.strptime((string + ' 08:00:00'), '%d%b%y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S')


def get_itm_iv(opts, expiry, spot):
    df1 = opts[opts.expiry == expiry]
    closest_strike = get_closest_val(df1.strike.values.tolist(), spot)
    print(f"{closest_strike=},{spot}")
    return df1[df1.strike == closest_strike]['iv'].iloc[0]


def get_next_friday_expiry(date: dt.datetime) -> dt.datetime:
    if date.weekday() == 4:
        if date.hour < 8:
            friday = date
        else:
            friday = date + dt.timedelta(days=7)
    else:
        friday = date + dt.timedelta((4 - date.weekday()) % 7)
    return friday.replace(hour=8, minute=0, second=0, microsecond=0)
