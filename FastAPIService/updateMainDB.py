from dotenv import load_dotenv
import os
import twstock
# twstock.__update_codes()
load_dotenv()
import sqlite3
import datetime

class DB_Ctrl:
  def __init__(self):
    self.con = sqlite3.connect(os.getenv('DB_PATH'))
    self.cursorObj = self.con.cursor()
    self.buildTable()
  def buildTable(self):
    self.con.execute(
      """
      CREATE TABLE IF NOT EXISTS datas (
        id INTEGER PRIMARY KEY,
        company_id TEXT,
        date TEXT,
        capacity INTEGER, 
        turnover INTEGER, 
        open REAL, 
        high REAL,
        low REAL,
        close REAL,
        change REAL, transaction_ REAL
      );
      """
    )
    self.con.commit()
  def rewriteDBData(self, company_id):
    stock = twstock.Stock(company_id)
    for data in stock.fetch_31():
      S_sql = '''
      INSERT OR REPLACE INTO datas ( id, company_id, date, capacity, turnover, open, high, low, close, change, transaction_ ) VALUES ( 
      ( SELECT id FROM datas WHERE company_id = "{company_id}" AND date = "{date}" ), 
      "{company_id}", "{date}", 
      {capacity}, {turnover}, {open}, {high}, {low}, {close}, {change}, {transaction_} );
      '''.format(
        company_id=company_id, date=data[0].strftime("%Y-%m-%d %H:%M:%S"), 
        capacity=data[1], turnover=data[2], open=data[3], high=data[4], low=data[5], close=data[6], change=data[7], transaction_=data[8]
      )
      print(S_sql)
      self.con.execute(S_sql)
      self.con.commit()
if __name__ == '__main__':
  test = DB_Ctrl()
  test.rewriteDBData("2330")