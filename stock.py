#! python3
# -*- coding: utf-8 -*-


import pandas as pd
import numpy as np
from WindPy import *
import time
import multiprocessing
import multiprocessing.pool
from multiprocessing import Pool
import os
import random
from sqlalchemy import create_engine
import sys
import math
from datetime import datetime,timedelta
import threading
import glob
import pathlib
from collections import defaultdict


connect = create_engine('mysql+pymysql://wps.data:wind.123.com@10.106.17.146:3306/stock?charset=utf8')

wind_ny_sectid='6210000000000000' #Wind能源
wind_cl_sectid='6215000000000000' #Wind材料
wind_gy_sectid='6220000000000000' #Wind工业
wind_kxxf_sectid='6225000000000000' #Wind可选消费
wind_rcxf_sectid='6230000000000000' #Wind日常消费
wind_ylbj_sectid='6235000000000000' #Wind医疗保健
wind_jr_sectid='6240000000000000' #Wind金融
wind_xxjs_sectid='6245000000000000' #Wind信息技术
wind_dxfw_sectid='6250000000000000' #Wind电信服务
wind_gysy_sectid='6255000000000000' #Wind公用事业
wind_fdc_sectid='6260000000000000' #Wind房地产
sh_index_sectid='a001030108000000' #上证综合股指数成份
sz_index_sectid='a001030116000000' #深证成份指数成份
a_sectid='a001010100000000' #全部A股
cy_sectid='a001010r00000000' #创业板

DATETIME_FORMAT='%Y%m%d'
DATETIME_YYYY_MM_DD_FORMAT='%Y-%m-%d'


class Stock():

    def __init__(self,start=None,end=None):
        self.A_WINDCODES = ''
        self.data_path = os.path.abspath('./data')
        self.dapan_filename = 'dapan.csv' # 大盘 股票windcode
        self.a_cy_filename = 'all_wind_code.csv' # A股创业股windcode
        self.windtype_filename = 'wind_type_stock.csv' # wind行业分类 1类 wincode
        self.wind_stock_filename = 'wind_stock.csv' # A股创业股 行情 start end
        self.start=start # 开始时间
        self.end=end # 结束时间
        if self.end is None:
            self.end = datetime.now().strftime(DATETIME_FORMAT)
        if self.start is None:
            self.start = (datetime.strptime(self.end, DATETIME_FORMAT) - timedelta(30)).strftime(DATETIME_FORMAT)
        self.init_df()

    def init_df(self):

        """
        1. 首先 初始化 df 根据 本地csv
        :return:
        """
        self.dapan_df = self.get_dapan_windcodes()
        self.windtype1_df = self.get_windtype_windcodes()
        self.get_a_chuang_windcodes()


    def load_data(self):

        """
        加载各个股票数据
        :return:
        """
        windstock_path = os.path.join(self.data_path, self.wind_stock_filename)
        if os.path.exists(windstock_path):
            self.wind_stock_df = pd.read_csv(windstock_path,sep=',',header=0)
        else:
            raise Exception('没有windstock.csv文件，先下载')

        # start end
        if self.end is None:
            self.end = datetime.now().strftime(DATETIME_FORMAT)
        if self.start is None:
            self.start = (datetime.strptime(self.end,DATETIME_FORMAT) - timedelta(30)).strftime(DATETIME_FORMAT)
        # 时间过滤
        start_date = self.start
        end_date = self.end
        if len(self.start) == 8 and self.start.count('-') == 0:
            start_date = datetime.strptime(self.start,DATETIME_FORMAT).strftime(DATETIME_YYYY_MM_DD_FORMAT)
        if len(self.end) == 8 and self.end.count('-') == 0:
            end_date = datetime.strptime(self.end,DATETIME_FORMAT).strftime(DATETIME_YYYY_MM_DD_FORMAT)
        self.wind_stock_df['DTATE_TIME'] = pd.to_datetime(self.wind_stock_df['DTATE_TIME'])
        date_filter = (self.wind_stock_df['DTATE_TIME']>start_date) &(self.wind_stock_df['DTATE_TIME']<=end_date)

        # 去掉 ST df['shortname'].str.contains('ST') == False
        no_st_wind_codes = list(self.windtype1_df.loc[self.windtype1_df['sec_name'].str.contains('ST') == False]['wind_code'])
        no_st_filter = self.wind_stock_df['WIND_CODE'].isin(no_st_wind_codes)
        self.wind_stock_df = self.wind_stock_df.loc[date_filter & no_st_filter]
        # 统一 成交额 单位为 亿  round(df['AMT']/100000000+1/100000000,2) 四舍五入
        self.wind_stock_df['AMT'] =  round(self.wind_stock_df['AMT']/100000000+1/100000000,2)

    def get_a_chuangye_sectid(self):
        """
        A 股和创业板 板块id
        :return:
        """

        return {a_sectid:'A股',
                cy_sectid:'创业板'}

    def get_dapan_sectid(self):
        """
        大盘板块id
        :return:
        """

        return {sh_index_sectid:'上证综合股指',
                sz_index_sectid:'深证成份指'}

    def get_windtype_sectid(self):
        """
        windtype板块id
        :return:
        """

        return {wind_ny_sectid:'Wind能源',
                wind_cl_sectid:'Wind材料',
                wind_gy_sectid:'Wind工业',
                wind_kxxf_sectid:'Wind可选消费',
                wind_rcxf_sectid:'Wind日常消费',
                wind_ylbj_sectid:'Wind医疗保健',
                wind_jr_sectid:'Wind金融',
                wind_xxjs_sectid:'Wind信息技术',
                wind_dxfw_sectid:'Wind电信服务',
                wind_gysy_sectid:'Wind公用事业',
                wind_fdc_sectid:'Wind房地产',}

    def get_windcodes_frm_wind(self, filename,**sectids):

        if sectids is None:
            raise Exception('板块不能为空')
        if filename is None:
            raise Exception('csv文件名字不能为空')
        dfs = []
        today = datetime.now().strftime('%Y-%m-%d')
        for sid,v in sectids.items():
            df = self.get_wset(today,sid)
            df['wind_type1'] = v
            if df is not None:
                dfs.append(df)
        df = pd.concat(dfs)
       # df.index = pd.RangeIndex(0,len(df))
        if not os.path.exists(self.data_path):
            os.chdir('.')
            os.mkdir(self.data_path)

        df.to_csv(os.path.join(self.data_path, filename))
        return df

    def get_a_chuang_windcodes(self):

        """
        获取A股和创业板所有股票windcode，如果本地没有则从wind获取，并且保存本地
        :return:
        """
        file = os.path.join(self.data_path, self.a_cy_filename)
        if not os.path.exists(file):
            windcodes = self.get_windcodes_frm_wind(self.a_cy_filename,**self.get_a_chuangye_sectid())
        else:
            windcodes = pd.read_csv(file,sep=',',header=0)

        self.A_WINDCODES = ','.join(list(windcodes['wind_code']))
        return windcodes

    def get_wset(self,s_date,sectid):

        """
        根据时间和板块获取数据
        :param date:
        :param sectid:
        :return:
        """
        r = w.wset("sectorconstituent", "date={};sectorId={}".format(s_date, sectid), usedf=True)
        if r[0] != 0:
            return None
        return r[1]

    def get_dapan_windcodes(self):
        """
        获取大盘windcode，本地没有则从wind拉取
        :return:
        """
        file = os.path.join(self.data_path, self.dapan_filename)
        if not os.path.exists(file):
            return self.get_windcodes_frm_wind(self.dapan_filename,**self.get_dapan_sectid())
        else:
            df = pd.read_csv(file,sep=',',header=0)
            return df

    def get_windtype_windcodes(self):
        """
        获取wind行业分类windcodes
        :return:
        """
        file = os.path.join(self.data_path, self.windtype_filename)
        if not os.path.exists(file):
            return self.get_windcodes_frm_wind(self.windtype_filename,**self.get_windtype_sectid())
        else:
            df = pd.read_csv(file,sep=',',header=0)
            return df


    def get_chg_volume_count_frm_wind(self,dtime,codes=''):

        """
        通过直接发送wind命令获取数据，太慢 暂时不用
        获取上涨跌平家数, 如果codes='' 则默认全部A股股票，剔除 退市
        :param dtime: eg :20200912
        :return:
        """
        if(codes == None or codes == '' or len(codes) == 0):
            codes = self.A_WINDCODES
        result = w.wss(codes, "chg,volume,trade_status", "tradeDate={};cycle=D".format(dtime), usedf=True)
        error_code = result[0]
        if error_code != 0:
            print('报错,错误码:{}'.format(error_code))
            return -1,-1,-1 # 涨家数，跌家数，平家数,
        df = result[1] # 数据

        #为了统计方便 增加一列 值都为 1
        df['CHG_FLAG']=1
        # 统计
        # 上涨 (df['trade_status'] == '交易') --去掉停牌
        df = df[df['TRADE_STATUS'] == '交易']
        up_chg_count = df.loc[(df['CHG'] > 0)]['CHG_FLAG'].value_counts()
        # 下跌
        down_chg_count = df.loc[(df['CHG'] < 0)]['CHG_FLAG'].value_counts()
        # 平
        no_chg_count = df.loc[(df['CHG'] == 0.0)]['CHG_FLAG'].value_counts()

        # 成交量
        volume = df['VOLUME'].fillna(0).sum()

        if len(up_chg_count)>0:
            up = up_chg_count[1]
        else:
            up = 0

        if len(down_chg_count)>0:
            down = down_chg_count[1]
        else:
            down = 0

        if len(no_chg_count)> 0:
            flat = no_chg_count[1]
        else:
            flat = 0
        return {'up':up,'down':down,'flat':flat,'volume':volume}


    def get_chg_amt_count_frm_df(self,codes=None):

        """
         根据本地csv
         获取上涨跌平家数及成交额
        :return:
        """
        df = self.wind_stock_df

        if codes is not None:
            df = self.wind_stock_df.loc[self.wind_stock_df['WIND_CODE'].isin(codes)]

        #为了统计方便 增加一列 值都为 1
        df['CHG_FLAG']=1
        # 统计
        # 上涨 (df['trade_status'] == '交易') --去掉停牌
        df = df[df['TRADE_STATUS'] == '交易']
        up_chg_count = df.loc[(df['CHG'] > 0)].groupby(by='DTATE_TIME')['CHG_FLAG'].sum()
        # 下跌
        down_chg_count = df.loc[(df['CHG'] < 0)].groupby(by='DTATE_TIME')['CHG_FLAG'].sum()
        # 平
        no_chg_count = df.loc[(df['CHG'] == 0.0)].groupby(by='DTATE_TIME')['CHG_FLAG'].sum()

        # 成交额
        amount = df.fillna(0).groupby(by='DTATE_TIME')['AMT'].sum()

        return pd.DataFrame({'up':up_chg_count,'down':down_chg_count,'flat':no_chg_count,'amount':amount})

    def get_chg_amt_up_frm_df(self):

        """
         根据本地csv
         获取上涨跌平及成交额 的 股票
        :return:
        """
        df = self.wind_stock_df

        #为了统计方便 增加一列 值都为 1
        df['CHG_FLAG']=1
        # 统计
        # 上涨 (df['trade_status'] == '交易') --去掉停牌
        df = df[df['TRADE_STATUS'] == '交易']
        up_chg_df = df.loc[(df['CHG'] > 0)]
        up_chg_df['type'] = '上涨'
        # 下跌
        down_chg_df = df.loc[(df['CHG'] < 0)]
        down_chg_df['type'] = '下跌'
        # 平
        no_chg_df = df.loc[(df['CHG'] == 0.0)]
        no_chg_df['type'] = '平'

        # 合并
        df = pd.concat([up_chg_df,down_chg_df,no_chg_df])
        return df


    def get_dapan_chg_amt_count(self):

        """
        计算大盘数 涨跌平家数,成交量
        :param dtime:
        :return:
        """

        windcode_df  = list(self.dapan_df['wind_code'])
        df = self.get_chg_amt_count_frm_df(windcode_df)
        df['type'] = '大盘'
        return df

    def get_windtype_chg_amt_count(self):

        """`
        计算wind1类 各个板块 涨跌平家数,成交量
        :param dtime:
        :return:
        """
        wind_type_code_df  = self.windtype1_df
        wind_types =  list(wind_type_code_df['wind_type1'].drop_duplicates())
        res_dict = []
        for w_type in wind_types:
            type_df = wind_type_code_df.loc[wind_type_code_df['wind_type1']==w_type]['wind_code']
            ddf= self.get_chg_amt_count_frm_df(list(type_df)).fillna(0)
            ddf['type']=w_type
            res_dict.append(ddf)

        return  pd.concat(res_dict)

    def get_maxup_stock(self):

        """
        # 列出板块 涨停股
        :return:
        """
        df = self.wind_stock_df.sort_values(["WIND_CODE","DTATE_TIME"])
        df['CONTINOUS_FLAG']=pd.RangeIndex(0,len(df))
        maxup_df = df.loc[df['MAXUPORDOWN'] ==1] #1 涨停
        type_df = self.windtype1_df
        df = pd.merge(left=maxup_df,right=type_df,how='inner',left_on='WIND_CODE',right_on='wind_code',left_index=True)
        df = df.loc[df['sec_name'].str.contains('ST') == False]
        return df

    def get_zha_maxup_stock(self):

        """
        炸板
        """
        df = self.wind_stock_df
        # 炸板 ,最高价=涨停价 收盘价< 涨停价
        zha_maxup_df = df.loc[(df['HIGH3']==df['MAXUP']) & (df['CLOSE3'] < df['MAXUP'])]
        return zha_maxup_df

    def get_first_maxup_stock(self):

        """
        首次涨停 算法：找到num=1 的windcode 就是首板
        :return:
        """

        maxup_df = self.get_maxup_stock()

        maxup_df['NUM'] = 1 # 为数量统计 增加一列 为1
        maxup_sum_s = maxup_df.groupby(by='WIND_CODE')['NUM'].sum()
        # sum == 1 的为首板
        first_s = maxup_sum_s.loc[maxup_sum_s ==1]

        # 返回
        first_df = maxup_df.loc[maxup_df['WIND_CODE'].isin(first_s.index)]

        return  first_df

    def get_lianban_maxup_stock(self):
        """
        算法描述：
            1. 计算出涨停 所有股票
            2. 在涨停股票中找出 多次涨停的 股票，即 windcode 数量》1的
            3. 根据 wincode 分组计算 算法见show_maxup 函数
        """
        df = self.get_maxup_stock()
        multi_windcode_s = df.value_counts('WIND_CODE') > 1
        multi_windcode_s = multi_windcode_s.loc[multi_windcode_s == True]
        multi_maxup_df = df.loc[df['WIND_CODE'].isin(multi_windcode_s.index)]
        t_df = []
        multi_maxup_df.groupby(by=['WIND_CODE']).apply(lambda x : self.show_maxup(x,t_df))


        return pd.concat(t_df)

    def show_maxup(self, s_df, df_list):

        """
        算法描述：
            1. 给同一个windcode 的df 增加连续序列，从1 到len（df）长度
            2. 与 整个CONTINOUS_FLAG 连续序列， 见get_maxup_stock 做差 ，得出的值相同的 就是连续的
            3. IS_CONTINUOUS_FLAG 存储了 差值， 找出差值相同的 方法： 该列 差值 数量 》1的 就是是连续的
        """
        ss_df = s_df.copy(deep=True)
        ss_df['C_INDEX'] = range(1,len(ss_df)+1)
        ss_df['IS_CONTINUOUS_FLAG'] = ss_df['CONTINOUS_FLAG'] - ss_df['C_INDEX']
        s_con=ss_df.value_counts('IS_CONTINUOUS_FLAG')
        s_con=s_con.loc[s_con > 1]
        c_df = ss_df.loc[ss_df['IS_CONTINUOUS_FLAG'].isin(s_con.index)]
        if(len(c_df) > 0):
            df_list.append(c_df)

    def get_data_frm_wind(self,temp_dir,windcodes,wind_date_map):

        '''
        从wind 获取数据
        :param temp_dir: 保存路径
        :param windcodes: windcodes
        :param wind_date_map: windcode对应的起止日期
        :return:
        '''
        # 随机休息
        # time.sleep(random.randint(1,))
        print("time:{},wcode:{},start get_data_frm_wind".format(datetime.now(),','.join(windcodes)))
        for wind_code in windcodes:
            start_time = wind_date_map[wind_code](0)
            end_time  = wind_date_map[wind_code](1)
            df_path="{}\\{}_{}_{}.csv".format(temp_dir,wind_code,start_time,end_time)
            if not os.path.exists(df_path):
                df=w.wsd(wind_code, "windcode,amt,chg,high3,low3,close3,trade_status,maxup,maxupordown", start_time, end_time, "", usedf=True)
                # df[1].reset_index(inplace=True)
                df[1].rename(columns={'windcode':'WIND_CODE'})
                df[1].to_csv(df_path)
                print('save windcode:{},to path:{} ok'.format(wind_code,df_path))
                time.sleep(random.randint(1,2))

    def get_wincode_date_map(self, data_dir):

        '''
        根据给定路径 ，获取改路径下 每个windcode 对应的start end time 目的，节省流量
        :param data_dir:
        :return:
        '''
        wincode_date_dict = defaultdict(set)
        # csv 文件的名字
        filenames = (pathlib.PurePath(f).stem for f in glob.glob('{}\\*.csv'.format(data_dir)))
        for f in filenames:
            wcode,start_date,end_date = f.split('_')
            if wcode is None:
                continue
            if start_date is None:
                start_date = self.start
            if end_date is None:
                end_date = self.end
            wincode_date_dict[wcode].add(start_date,end_date)

        # 根据csv文件名字中的时间，计算起止时间, set 中保存 时间最大的那个
        for k,v in wincode_date_dict.items():
            wincode_date_dict[k]=set(min(max(v),self.start),max(max(v),self.end))
        wcodes1 = set(wincode_date_dict.keys())
        wcodes2 = set(self.A_WINDCODES)

        wcodes3 = wcodes2.difference(wcodes1)
        if wcodes3 is not None and len(wcodes3) > 0:
            for wcode in wcodes3:
                wincode_date_dict[wcode].add(self.start,self.end)
        return  wincode_date_dict




    def get_sdata_bythread_frm_wind(self,startDate=None,endDate=None):
        threads = []

        if startDate is None and self.start is not  None:
            startDate = self.start
        if endDate is None and self.end is not  None:
            endDate = self.end


        # 以当前时间建立文件夹
        data_dir = os.path.join(self.data_path, 'data_{}'.format(datetime.now().strftime(DATETIME_FORMAT)))
        if not os.path.exists(data_dir):
            os.mkdir(data_dir)

        codes = self.A_WINDCODES.split(',')
        grp_size = 8
        per_size = math.ceil(len(codes)/grp_size)
        groups = [codes[i:i + per_size] for i in range(0, len(codes), per_size)]

        for i in range(grp_size):
            wind_codes = groups[i][:]
            t = threading.Thread(target=self.get_data_frm_wind,args=(data_dir,wind_codes,self.get_wincode_date_map(data_dir)))
            threads.append(t)

        for i in range(grp_size):
            time.sleep(i*3)
            threads[i].start()

        for i in range(grp_size):
            threads[i].join()

        print('all pull from wind ok')
        print('merge start')
        self.merge_csv(data_dir)
        print('merge ok')


    def merge_csv(self,datadir):

        # 合并
        temp_path = os.path.join(datadir,'*.csv')# './data/temp/*.csv'
        print('merge {}.*csv'.format(temp_path))
        # 只合并 startdate enddate 的csv
        paths = glob.glob(temp_path)
        merge_df = pd.concat((self._fix_df(pd.read_csv(p,sep=',',header=0),p) for p in paths))
        merge_df.rename(columns={'Unnamed: 0':'DATE_TIME'},inplace=True)
        merge_df.index=pd.RangeIndex(0,len(merge_df))
        csv_path = os.path.join(self.data_path,self.wind_stock_filename)
        merge_df.to_csv(csv_path)
        print('save all stock to {}'.format(csv_path))
        return merge_df

    def _fix_df(self,df,filname):
        df['WIND_CODE'] = os.path.basename(filname).strip().rstrip('.csv').strip()
        return  df

    def import_data_to_mysqldb(self, windcodes):
        from sqlalchemy import create_engine

        """
        将数据从cvs 导入到mysql数据库
        :param windcodes:
        :return:
        """
        connect = create_engine('mysql+pymysql://wps.data:wind.123.com@10.106.17.146:3306/stock?charset=utf8')
        for wind_code in windcodes:
            csv_path = "./data/{}.csv".format(wind_code)
            if not os.path.exists(csv_path):
                continue
            df=pd.read_csv(csv_path,sep=',',header=0)
            df['WIND_CODE'] = wind_code
            df.rename(columns={'Unnamed: 0':'DTATE_TIME'},inplace=True)
            pd.io.sql.to_sql(df,'wind_stock',connect,if_exists = "append",index  = False)
            print('time:{},wcode:{},ok'.format(datetime.now(),wind_code))
        print('t:{},codes:{}'.format(datetime.now(),','.join(windcodes)))

    def import_data_to_sqlitedb(self,cpath=None):
        from sqlalchemy import create_engine

        """
        将数据从cvs 导入到mysql数据库
        :return:
        """
        os.chdir('.')
        windcodes = self.A_WINDCODES.split(',')
        connect = create_engine('sqlite:///stock.db')
        for wind_code in windcodes:
            if cpath == None:
                csv_path = "./data/{}.csv".format(wind_code)
            else:
                csv_path = "./data/{}/{}.csv".format(cpath, wind_code)
            if not os.path.exists(csv_path):
                continue
            df=pd.read_csv(csv_path,sep=',',header=0)
            df['WIND_CODE'] = wind_code
            df.rename(columns={'Unnamed: 0':'DTATE_TIME'},inplace=True)
            pd.io.sql.to_sql(df,'wind_stock',connect,if_exists = "append",index  = False)
            print('time:{},wcode:{},ok'.format(datetime.now(),wind_code))
        print('t:{},codes:{}'.format(datetime.now(),','.join(windcodes)))

    def multi_import_mysql_data(self):

        """
        多进程导数据到数据库
        :return:
        """

        codes = self.A_WINDCODES.split(',')
        per_size = 600
        groups = [codes[i:i+per_size] for i in range(0,len(codes),per_size)]

        pool = Pool(len(groups))
        for wind_codes in groups:
            time.sleep(2)
            pool.apply(self.import_data_to_mysqldb,(wind_codes,))


        pool.close()
        pool.join()

        print('all ok')



if __name__ == '__main__':

    """:arg
    :arg[0] python 文件名
    :arg[1] 函数代号 D-从wind下载数据
    :arg[2] :arg[3] 函数参数 以此类推
    """
    os.chdir('.')
    w.close()
    w.start()

    s = Stock()

    py_file = sys.argv[0]
    print(py_file)

    method = sys.argv[1]
    if method == 'D': # 从wind 下载数据
        start = sys.argv[2]
        end = sys.argv[3]

        grp_no = int(sys.argv[4])

        s.get_data_4(start,end,grp_no)
    elif method == 'IS': # 数据导入sqlite
        csv_path = sys.argv[2]
        s.import_data_to_sqlitedb(csv_path)
    elif method == 'IM': # 导入mysql
        s.multi_import_mysql_data()
    elif method == 'DM': # 多线程拉数据 从wind 下载数据
        start = end = ''
        if len(sys.argv) >= 3:
            start = sys.argv[2]
        if len(sys.argv) >= 4:
            end = sys.argv[3]
        if start  == '':
            start = None
        if end  == '':
            end = None

        s.get_sdata_bythread_frm_wind(start, start)