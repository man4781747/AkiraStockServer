# https://fastapi.tiangolo.com/zh/tutorial/metadata/

from typing import Optional
from fastapi import FastAPI, Request, Form, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
from dotenv import load_dotenv
load_dotenv()
from fastapi.middleware.cors import CORSMiddleware
import sqlite3

S_mainDB_path = os.getenv('DB_PATH')
con = sqlite3.connect(S_mainDB_path)
# App物件建立
# Swagger 路徑設定
mainApp = FastAPI(docs_url='/AkiraBackend') 

origins = ["*"]


# CORS 設定
mainApp.add_middleware(
  CORSMiddleware,
  allow_origins=origins,
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

# template 資料夾設定
# S_template_folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "template")
# template 路徑設定
# templates = Jinja2Templates(directory=S_template_folder)

# static 資料夾設定
# S_static_folder   = os.path.join(os.path.dirname(os.path.realpath(__file__)), "static")
# static url 設定 
# mainApp.mount("/static", StaticFiles(directory=S_static_folder), name="static")

# App 執行
# runApp(mainApp)

# @mainApp.get("/") # 跟目錄設定打開html
# def read_root(request: Request):
#   return templates.TemplateResponse("index.html",{"request": request})

@mainApp.get("/") # 跟目錄設定打開html
def read_root(request: Request):
  return "Hello World!"

