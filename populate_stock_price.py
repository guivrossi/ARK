from logging import currentframe
import config
import alpaca_trade_api as tradeapi
import psycopg2
import psycopg2.extras
import csv
import datetime as dt

connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER, 
    password=config.DB_PASS
    )

# import os

# cwd = os.getcwd()  # Get the current working directory (cwd)
# files = os.listdir(cwd)  # Get all the files in that directory
# print("Files in %r: %s" % (cwd, files))

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)


cursor.execute("select * from stock WHERE is_etf=TRUE")

etfs = cursor.fetchall()

today = dt.date.today()

for etf in etfs:
    print(etf['symbol'])
    
    with open(f"ARK INVEST/data/{etf['symbol']}.csv") as f:
        reader = csv.reader(f)
        for row in reader:
            ticker = row[3]
            
            if ticker:
                shares = row[5]
                weight = row[7]
                
                cursor.execute("""
                    SELECT * FROM stock WHERE symbol = %s
                
                """, (ticker,)
                )
                stock = cursor.fetchone()
            
                if stock:
                    cursor.execute("""
                        INSERT INTO etf_holding (etf_id, holding_id, dt, shares, weight)
                        VALUES (%s, %s, %s, %s, %s) 
                    """, (etf['id'], stock['id'], today, shares, weight)
                    )
    
connection.commit()