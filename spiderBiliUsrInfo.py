import requests
import json
import random
import pymysql
import datetime
import time
from multiprocessing.dummy import Pool as ThreadPool
import fakerHeaders
import re


# userApi = "https://api.bilibili.com/x/space/acc/info?mid=8621739&jsonp=jsonp"


def datetime_to_timestamp_in_milliseconds(d):
    """
    todo : 时间戳
    :param d:
    :return:
    """

    def current_milli_time():
        return int(round(time.time() * 1000))

    return current_milli_time()


def divide(thread_i_di):
    """
    todo : 分配任务哈
    :param thread_i_di:
    :return:
    """
    for j in range(0, total//thread):


        userNumber = j*thread + (thread_i_di + 1)
        if (startMid < userNumber):
                commitUrl = "https://space.bilibili.com/" + str(userNumber)
                getUserInfoJsonContent(commitUrl)
                time.sleep(0.5)
                print("sprider has run {}.".format(userNumber))


def getUserInfoJsonContent(url):
    """
    todo : 获取当前url对应的用户基本信息 (name sex sing mid ..)
    :param url: 请求url
    :return: post 返回对象 response
    """

    hideHeader = fakerHeaders.getFakerHeaders()
    postUrl = 'http://space.bilibili.com/ajax/member/GetInfo'
    userNumber = re.findall(r"\d+", url)

    payLoad = {
            '_' : datetime_to_timestamp_in_milliseconds(datetime.datetime.now()),
            'mid' : url.replace('https://space.bilibili.com/', '') }

    head = {
            'User-Agent': hideHeader,
            'Referer': 'https://space.bilibili.com/' + str(userNumber[0]) + '?from=search&seid=' + str(random.randint(10000, 50000))}

    #print(userNumber[0])
    #print("url:", url)
    #print("payLOad: ", payLoad)
    #print("head: ", head)

    try:
        response = requests.session().post(postUrl, headers=head, data=payLoad, timeout = 9)
        time.sleep(1.5)
        print("response: ",response)

        if ( response.status_code == 200 ):

            parserText(response)
            #print(response.text)
            #print("successfuly!:",url)
            return True
        elif(response.status_code == 412):
            time.sleep(3600)
            getUserInfoJsonContent(url)
        else:

            print("none, try again. coed: ", response.status_code)
            getUserInfoJsonContent(url)
            return False
    except:

        print("error, try again.Retry URL:",url )#, "  code:", response.status_code)
                                                 #wrong :ocal variable 'response' referenced before assignment
        getUserInfoJsonContent(url)
        return False


def getUserFansJsonContent(midNumber):
    """
    todo : 根据 userMid 请求对应fans内容
    :param midNumber: 用户 mid
    :return: 请求返回对象responseView
    """

    hideHeader = fakerHeaders.getFakerHeaders()
    head = { 'User-Agent': hideHeader  }
    fanUrl = 'https://api.bilibili.com/x/relation/stat?vmid=' + str(midNumber) + '&jsonp=jsonp'

    try:
        responseFans = requests.session().get(fanUrl, headers=head, timeout = 7)
        time.sleep(1)
        if ( responseFans.status_code == 200 ):
            #print("successfuly get fan info! midnumber:",midNumber)
            return responseFans
        else:
            print("none, try again to get fan info. code: ", responseFans.status_code)
            getUserFansJsonContent(midNumber)
            return False
    except:
        print("error, try again.Retry to get fan info, midnumber:",midNumber)
        getUserFansJsonContent(midNumber)
        return False


def getUserViewJsonContent(midNumber):
    """
    todo : 根据 userMid 请求对应View内容
    :param midNumber: 用户 mid
    :return: 请求返回对象responseView
    """

    hideHeader = fakerHeaders.getFakerHeaders()
    head = { 'User-Agent': hideHeader  }
    viewUrl = 'https://api.bilibili.com/x/space/upstat?mid=' + str(midNumber) + '&jsonp=jsonp'

    try:
        responseView = requests.session().get(viewUrl, headers=head, timeout = 7)
        time.sleep(1)
        if ( responseView.status_code == 200 ):
            #print("successfuly get view info! midnumber:",midNumber)
            return responseView
        else:
            print("none, try again to get view info.status_code: ", responseView.status_code)
            getUserViewJsonContent(midNumber)
            return False
    except:
        print("error, try again.Retry to get view info, midnumber:",midNumber)
        getUserViewJsonContent(midNumber)
        return False


def parserText(response):
    """
    todo : 解析
    :param response:
    :return:
    """
    try:
        jsDict = json.loads(response.text)
        statusJson = jsDict['status'] if 'status' in jsDict.keys() else False
        #print(statusJson)
        if statusJson == True:
            if 'data' in jsDict.keys():
                jsData = jsDict['data']
                mid = jsData['mid'] # 1
                name = jsData['name'] #2
                sex = jsData['sex'] #3
                rank = jsData['rank'] #4
                face = jsData['face'] #5
                regtimestamp = jsData['regtime'] if 'regtime' in jsData.keys() else 0
                regtime_local = time.localtime(regtimestamp)
                regtime = time.strftime("%Y-%m-%d %H:%M:%S", regtime_local) #6
                spacesta = jsData['spacesta'] #7
                birthday = jsData['birthday'] if 'birthday' in jsData.keys() else 'nobirthday' #8
                sign = jsData['sign'] #9
                level = jsData['level_info']['current_level'] #10
                OfficialVerifyType = jsData['official_verify']['type'] #11
                OfficialVerifyDesc = jsData['official_verify']['desc'] #12
                vipType = jsData['vip']['vipType'] #13
                vipStatus = jsData['vip']['vipStatus'] #14
                toutu = jsData['toutu'] #15
                toutuId = jsData['toutuId'] #16
                coins = jsData['coins'] #17

                # print("run here：" , name,mid)

                responseFan = getUserFansJsonContent(mid)
                if (responseFan.status_code == 200):
                    js_fans_data = json.loads(responseFan.text)
                    userFollowing = js_fans_data['data']['following']  # 18
                    userFans = js_fans_data['data']['follower']  # 19
                    # print("here:", userFans, userFollowing)
                else:
                    pass

                responseView = getUserViewJsonContent(mid)
                if (responseView.status_code == 200):
                    js_viewdata = json.loads(responseView.text)
                    userArchiveView = js_viewdata['data']['archive']['view']  # 20
                    userArticleView = js_viewdata['data']['article']['view']  # 21
                    # print("here: ", userArticleView, userArchiveView)
                else:
                    pass
                insertIntoDataBase(mid, name, sex, rank, face, regtime, spacesta, birthday, sign, level,
                                   OfficialVerifyType, OfficialVerifyDesc, vipType, vipStatus, toutu, toutuId, coins,
                                   userFollowing, userFans, userArchiveView, userArticleView)
                print("successfully.prefer into sleep 0.5s. mid : ", mid)
                # print(mid, name, sex, rank, face, regtime, spacesta, birthday, sign, level, OfficialVerifyType, OfficialVerifyDesc,vipType, vipStatus, toutu, toutuId, coins,userFollowing, userFans, userArchiveView, userArticleView)
                time.sleep(1)
    except:
        pass


def insertIntoDataBase(s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21):
    # 打开数据库连接
    db = pymysql.connect("localhost", "root", "123456", "Bilili_Comment", charset='utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    # SQL 插入语句
    sql = "INSERT INTO First_Get_UserInfo_Thread" \
          "(mid,       name,   sex,     rank,    face,   regtime, spacesta, birthday, sign,   level,  OfficialVerifyType, OfficialVerifyDesc,vipType,  vipStatus, toutu,    toutuId,  coins,     userFollowing, userFans, userArchiveView, userArticleView) VALUES " \
          "(%s,       '%s',    '%s',    %s,      '%s',     '%s',     %s,      '%s',   '%s',    %s,     %s,                '%s',              %s,        %s,       '%s',     %s,       %s,        %s,            %s,       %s,             %s            )" \
          % (int(s1), str(s2), str(s3), int(s4), str(s5), str(s6), int(s7), str(s8), str(s9), int(s10),int(s11),          str(s12),          int(s13), int(s14),  str(s15), int(s16), int(s17), int(s18),       int(s19), int(s20),         int(s21)       )
    #print(sql)
    try:
        # 执行sql语句
        #print("prefer execute sql.  ")
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
        # 关闭数据库连接
        db.close()
    except:
        print("worng in insert")
        # 如果发生错误则回滚
        db.rollback()
        db.close()


def getProxyIPFromMy():
    """
             用redis中的IP，并用函数－checkIP（）监测
    :return: 自建代理ip
    """
    PROXY_POOL_URL = 'http://localhost:5555/random'
    try:
         response = requests.get(PROXY_POOL_URL)
         return response.text

    except :
        return False



if __name__ == '__main__':


    # 线程
    thread = 6
    # url 总数
    total = 100000

    ###
    startMid = 89061


    time0 = time.time()

    thread_i = [j
             for j in range(0, thread)
             ]

    pool = ThreadPool(thread)
    pool.map(divide, thread_i)

    pool.close()
    pool.join()
    time1 = time.time()
    print("总花费时间:{}s".format(time1-time0))


