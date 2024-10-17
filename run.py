import yfinance as yf
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from scipy import stats
from datetime import datetime, timedelta
import logging
import logging.handlers
from tqdm import tqdm
import time
import os
import re
import base64
import hashlib
import maskpass

import create
import app
import find

# app.analyze_stocks()
# create.create_tables()
# find.retrieve_and_display_stocks()


def main():
    try:
        create.create_tables()
        app.analyze_stocks()
        find.retrieve_and_display_stocks()
    except Exception as e:
        print(f"exception {e}")


def password():
    pwd = maskpass.askpass(prompt="Password:", mask="*")  


#sha512