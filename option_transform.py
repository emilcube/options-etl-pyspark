from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, lit
import pyspark.sql.types as types
import pyspark.sql.functions as func
from functools import reduce
from pyspark.sql import DataFrame
import functools
from py_vollib.black_scholes.implied_volatility import implied_volatility
import py_vollib.black_scholes.greeks.numerical
import glob
import os
from math import sqrt
import util
from pyspark.sql import SQLContext, Window

def error_handler(func):
    def inner_func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            return 0.0

    return inner_func

@error_handler
def calc_iv(price, spot_price, strike, maturity, itype):
    return float(implied_volatility(price * spot_price,
                                            spot_price,
                                            strike,
                                            maturity,
                                            0.02,
                                            itype
                                            ))
@error_handler
def calc_rtv(itype, spot_price, strike, maturity, iv, trade_amt):
  #return float((float(py_vollib.black_scholes.greeks.numerical.vega(itype, spot_price, strike, maturity, 0.02, iv))*trade_amt)/sqrt(maturity))
  return float(util.bs_rtv(spot_price, strike, maturity, 0.02, iv)*trade_amt)
  #vega * trade_amount / sqrt(time_to_maturity)
  

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)
def unionAlls(dfs):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)

def TransformPrice(df_price):
    df_price = df_price.withColumnRenamed('_c0', 'time_pr')
    df_price = df_price.withColumnRenamed('_c1', 'spot_price')

    df_price = df_price.withColumn('ts_new', F.to_timestamp('time_pr', 'yyyy-MMM-dd HH:mm:ss')) # df_price =
    df_price = df_price.drop('time_pr')
    df_price = df_price.withColumnRenamed('ts_new', 'time_pr')
    return df_price

def TransformOption(df, df_price):
    columns_to_drop = ['timestamp', 'id']
    df = df.drop(*columns_to_drop)

    df = df.withColumn('tmp_time', (F.col('local_timestamp') / 1000000).cast(types.IntegerType()))
    df = df.withColumn('time', func.from_unixtime('tmp_time').cast(types.TimestampType()))
    df = df.drop('tmp_time')
    df = df.drop('local_timestamp')

    '''
    sideConvert = udf(lambda x: 1 if x == "buy" else 0, types.IntegerType())
    df = df.withColumn("side2", sideConvert(F.col("side")))
    df = df.drop('side')
    df = df.withColumnRenamed('side2', 'side')
    '''
    df = df.withColumn('maturity', F.split(df['symbol'], '-').getItem(1)) \
            .withColumn('strike', F.split(df['symbol'], '-').getItem(2).cast(types.IntegerType())) \
            .withColumn('itype', F.split(df['symbol'], '-').getItem(3))

    '''
    tmp_df = df.select(F.col("symbol")).limit(1) #.show()
    s = ''.join(d['symbol'] for d in tmp_df.collect())
    splitRes = s.split("-")
    print(splitRes)
    ftok = splitRes[0]
    '''
    df = df.join(df_price, func.date_trunc("minute", df.time) == df_price.time_pr ,"inner")
    df = df.withColumn('maturity_ts', F.to_timestamp('maturity', 'ddMMMyy') + F.expr('INTERVAL 8 HOURS'))
    seconds_in_year = 60 * 60 * 24 * 365
    df = df.withColumn('DiffInSeconds', (col("maturity_ts").cast("long") - col('time').cast("long")) / seconds_in_year)

    #ivCalcer = udf(lambda x: calc_iv(x, 'price'), types.DoubleType())
    ivCalcer = udf(calc_iv, types.DoubleType())
    df = df.withColumn("iv", ivCalcer(F.col("price"), F.col("spot_price"), F.col("strike"), F.col("DiffInSeconds"), F.lower(F.col("itype")))) #.limit(1) # maturity
    #print(df.show())

    rtvCalcer = udf(calc_rtv, types.DoubleType()) # util.bs_rtv FloatType
    df = df.withColumn("rtv", rtvCalcer(F.lower(F.col("itype")), F.col('spot_price'), F.col('strike'), F.col('DiffInSeconds'), F.col('iv'), F.col('amount'))) # *F.col('amount')
    #print(df.show())

    columns_to_drop = ['maturity', 'strike', 'itype', 'spot_price', 'time_pr', 'maturity_ts', 'DiffInSeconds']
    df = df.drop(*columns_to_drop)
    return df

def SUM_RTV(df):
  df = df.withColumn("trunc_time", F.date_trunc("hour", "time"))
  df = df.select(F.col("trunc_time"), F.col("rtv")) #.limit(5)
  df = df.groupBy('trunc_time').sum('rtv') #.orderBy('trunc_time', ascending=True)
  return df

def EMA_CALC(df):
  #w = Window().partitionBy("sum(rtv)").rowsBetween(-1,1) #orderBy(F.col("trunc_time"))
  #df = df.withColumn("MA",F.avg("sum(rtv)").over(w))
  data_sdf = df.withColumn("idx", F.monotonically_increasing_id())
  data_sdf = data_sdf.withColumn("gk", lit("gk"))

  data_sdf = data_sdf.withColumnRenamed('sum(rtv)', 'a')
  # create structs with all columns and collect it to an array
  # use the array of structs to do the val calcs
  arr_of_structs_sdf = data_sdf. \
  withColumn('allcol_struct', func.struct(*data_sdf.columns)). \
  groupBy('gk'). \
  agg(func.array_sort(func.collect_list('allcol_struct')).alias('allcol_struct_arr'))

  # a function to create struct schema string
  struct_fields = lambda x: ', '.join([str(x)+'.'+k+' as '+k for k in data_sdf.columns])

  # ema calculation. using `aggregate` to do the val calc
  data_sdf = arr_of_structs_sdf. \
      withColumn('new_allcol_struct_arr',
                func.expr('''
                          aggregate(slice(allcol_struct_arr, 2, size(allcol_struct_arr)),
                                    array(struct({0}, (allcol_struct_arr[0].a) as EMA)),
                                    (x, y) -> array_union(x, 
                                                          array(struct({1}, (0.02 * y.a + (1 - 0.02)*element_at(x, -1).EMA) as EMA))
                                                          )
                                    )
                          '''.format(struct_fields('allcol_struct_arr[0]'), struct_fields('y'))
                          )
                ). \
      selectExpr('inline(new_allcol_struct_arr)')#.show(truncate=False)
  data_sdf = data_sdf.withColumnRenamed("a", "sum(rtv)")
  data_sdf = data_sdf.drop("idx","gk")
  return data_sdf

def RTV_addition(df_cumm, df2):
  # more efficient would be outer join
  # df = df.join(df2,df.trunc_time ==  df2.trunc_time,"outer")
  df = df_cumm.union(df2)
  df = df.groupBy('trunc_time').sum('sum(rtv)') #.orderBy('trunc_time', ascending=True)
  return df
