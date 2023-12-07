from pyspark.sql import SparkSession
import pyspark.sql.types as types
from datetime import datetime
import glob
import os

import option_transform
from dotenv import load_dotenv
load_dotenv()

price_filepath = os.getenv("price_filepath")
opt_filepath = os.getenv("opt_filepath")
maskStr = os.getenv("maskStr")
baseOut = os.getenv("baseOut")
emaOut = os.getenv("emaOut")

if __name__ == "__main__":
  spark = SparkSession.builder.appName("options-etl-pyspark").getOrCreate()

  df_price = spark.read.csv(price_filepath, header=False, inferSchema=True) # load spot prices (for iv and rtv calc)
  df_price = option_transform.TransformPrice(df_price) 
  
  schema = types.StructType([
    types.StructField('trunc_time', types.TimestampType(), True),
    types.StructField('sum(rtv)', types.DoubleType(), True)
  ])
  df_rtv_accum = spark.createDataFrame([],schema)

  isExist = os.path.exists(baseOut)
  if not isExist:
    os.makedirs(baseOut)

  dr = glob.glob(maskStr)
  print(datetime.now(), "day_folder_options_start")
  for day_folder_options in dr:
    day_folder_options = day_folder_options.replace("\\","/")
    print(datetime.now(), day_folder_options, "start block")
    dr2 = glob.glob(day_folder_options + "/*.csv")
    frames = []
    for unit_option in dr2:
      unit_option = unit_option.replace("\\","/")
      df = spark.read.csv(unit_option, header=True, inferSchema=True)
      df = option_transform.TransformOption(df, df_price)
      frames.append(df)
    if len(dr2) > 0 and len(frames) > 0:
      dfDay = option_transform.unionAlls(frames) # df with all options trade in one specific day
      df_rtv_accum = option_transform.RTV_addition(df_rtv_accum, option_transform.SUM_RTV(dfDay))
      outFile = baseOut + "Deribit_option_trades_" + day_folder_options.rsplit("/", 1)[1] + ".csv" # Y-M-D
      df.repartition(1).write.csv(outFile) #df.to_csv(outFile)
  print(datetime.now(), "day_folder_options_end")

  df_rtv_accum = df_rtv_accum.orderBy('trunc_time', ascending=True)
  print(datetime.now(), "ema_calc_start")
  df_ema = option_transform.EMA_CALC(df_rtv_accum)
  print(datetime.now(), "ema_calc_end")
  df_ema.repartition(1).write.csv(emaOut)
  spark.stop()