
from pykiwoom.kiwoom import *
import time
import datetime
import pandas as pd
import os

try:
    if not os.path.exists("data"):
        os.mkdir("data")
except OSError:
    print ('Error: Creating directory. ' +  "data")


def df_pre(df):

    num_rows = df.shape[0]

    if num_rows <= 1:
        #데이터프레임의 행이 하나 이하
        pass

    else:

        # 데이터에 -가 있어서 -를 삭제시킴
        df['현재가']=df['현재가'].str.replace('-', '')
        df['현재가']=df['현재가'].astype('int')

        df['거래량']=df['거래량'].astype('int')

        df['시가']=df['시가'].str.replace('-', '')
        df['시가']=df['시가'].astype('int')

        df['고가']=df['고가'].str.replace('-', '')
        df['고가']=df['고가'].astype('int')

        df['저가']=df['저가'].str.replace('-', '')
        df['저가']=df['저가'].astype('int')

        df['체결시간'] = pd.to_datetime(df['체결시간'], format='%Y%m%d%H%M%S', errors='raise')


    return df


def tr_conti(code, set_d, tic):

    # TR 요청 (연속조회)
    dfs = []
    df = kiwoom.block_request("opt10080",
                            종목코드=code,
                            기준일자=set_d,
                            틱범위=tic,
                            수정주가구분=1,
                            output="주식분봉차트조회",
                            next=0)
    #print(df.head())
    dfs.append(df)

    while kiwoom.tr_remained:
        df = kiwoom.block_request("opt10080",
                                종목코드=code,
                                기준일자=set_d,
                                틱범위=tic,
                                수정주가구분=1,
                                output="주식분봉차트조회",
                                next=2)
        dfs.append(df)
        time.sleep(3.6)

    dfa = pd.concat(dfs)
    
    return df_pre(dfa)
    #return dfa

if __name__ == "__main__":

    # 키움 로그인
    kiwoom = Kiwoom()
    kiwoom.CommConnect(block=True)
    print("블록킹 로그인 완료")

    # 전종목 종목코드
    kospi = kiwoom.GetCodeListByMarket('0')
    #kosdaq = kiwoom.GetCodeListByMarket('10')
    #codes = kospi + kosdaq
    codes = kospi 

    # 문자열로 오늘 날짜 얻기
    now = datetime.datetime.now()
    today = now.strftime("%Y%m%d")

    # 전 종목의 일봉 데이터
    for i, code in enumerate(codes):
        print(f"{i}/{len(codes)} {code}")
        df = tr_conti(code=code,set_d=today,tic=5)

        out_name = f"{code}.csv"
        df.to_csv(f"./data/{out_name}", index=False, encoding= 'utf-8-sig')
        time.sleep(3.6)
