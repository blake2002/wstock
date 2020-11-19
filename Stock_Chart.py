#! python3
# -*- coding: utf-8 -*-

import pandas as pd
import stock
from pyecharts import Bar


class StockChart():

    def __init__(self, stock):
        self.stock = stock

    def get_up_down_flag_count_bar_chart(self):
        df = self.stock.get_dapan_chg_amt_count()


if __name__ == '__main__':
    pass
