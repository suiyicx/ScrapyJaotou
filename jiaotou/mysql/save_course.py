import shelve
import pymysql.cursors
import redis
import json
import time
from jiaotou.mysql.find_last import find_last


def save_course(redis_key, cid, start=0, end=-1):#cid从100000开始
    connect = pymysql.Connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='123456',
        db='course',
        charset='utf8'
    )
    cursor = connect.cursor()
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
    r = redis.Redis(connection_pool=pool)
    #获取redis数据库中redis_key列表元素，默认全部
    s = r.lrange(redis_key, start, end)
    index = start
    l1 = []
    l2 = []
    for i in s:
        if not i:
            continue
        item = json.loads(i.decode('utf-8'))
        if not item['name']:
            continue
        item['detail'] = find_last(u'}', item['detail'])
        if len(item['detail']) > 500:
            item['detail'] = item['detail'][:499]
               if "'" in item['detail']:
                  item['detail'] = '‘'.join(item['detail'].split("'"))
        if len(item['client']) > 50:
            item['client'] = item['client'][:49]
        for key in item:
            if isinstance(item[key], str):
                if "'" in item[key]:
                    item[key] = '‘'.join(item[key].split("'"))
        if item['type']:
            type1 = item['type'][0]
            type2 = item['type'][1]
        else:
            type1, type2 = '', ''
            if "'" in item['client']:
                 item['client'] = '‘'.join(item['client'].split("'"))
        sql = """
    INSERT INTO course (cid,name,cname,content,client,date,hour,type1,type2,price)
    VALUES ( '%d', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s')
    """
        data = (cid, item['name'], item['course'], item['detail'], item['client'], item['date'][0], item['date'][1],
                type1, type2, item['price'])
        try:
            cursor.execute(sql, data)
        #捕获由于sql语句错误抛出的异常
        except pymysql.err.ProgrammingError:
            date = time.strftime('%Y-%m-%d-%H%M%S', time.localtime(time.time()))
            #收集错误 redis key中元素对应索引和报错时间
            err = (index, date)
            l1.apend(err)
        #捕获主键冲突
        except pymysql.err.IntegrityError:
            date = time.strftime('%Y-%m-%d-%H%M%S', time.localtime(time.time()))
            err = (index, date)
            l2.apend(err)
        else:
            connect.commit()
            cid += 1
        finally:
            index += 1
    cursor.close()
    connect.close()
    date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    file1 = 'D:/jiaotou/school/mysql_err/sqlerr/%s.txt' % date
    file2 = 'D:/jiaotou/school/mysql_err/integrityerr/%s.txt' % date
    if l1:
        with open(file1, 'w') as f:
            f.write(str(l1))
    if l2:
        with open(file2, 'w') as f:
            f.write(str(l2))

