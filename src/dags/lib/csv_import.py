import pandas as pd
import numpy as np
import pathlib 
from sqlalchemy import create_engine

def import_gdp():
  rootdir = str(pathlib.Path(__file__).parent.absolute())+"/../.."

  df = pd.read_excel(rootdir+'/../data/gdp.xlsx', sheet_name="Table 5", header=1)

  df = df[~df['LA code'].isna()]
  df = df.drop(columns=["NUTS1 Region", "LA name"])
  df = df.rename(columns={"LA code": "la_code", "20183": "2018"})
  df = df.melt(id_vars=["la_code"], var_name="years", value_name="gdp")

  engine = create_engine("mysql://root:root@127.0.0.1:3307/test3")

  df.to_sql("gdp", engine, if_exists='replace')









