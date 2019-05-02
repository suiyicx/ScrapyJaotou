import pymysql.cursors
import redis
import json
import time
from jiaotou.mysql.find_last import find_last


#把redis数据库中数据保存到mysql
def save_school(redis_key, id, start=0, end=-1):#id从10000开始
    l1 = []
    l2 = []
    connect = pymysql.Connect(
        host='localhost',
        port=3306,
        user='root',
        passwd='123456',
        db='school',
        charset='utf8'
    )
    cursor = connect.cursor()
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
    r = redis.Redis(connection_pool=pool)
    s = r.lrange(redis_key, start, end)
    index = start
    for i in s:
        if not i:
            break
        item = json.loads(i.decode('utf-8'))
        if item['name']:
            if item['city']:
                item['intro'] = find_last(u'}', item['intro'])
                for key in item:
                    if isinstance(item[key], list):
                        item[key] = str(item[key])
                    if isinstance(item[key], str):
                        item[key] = item[key].replace("'", "\\\'")
                if len(item['intro']) > 1000:
                    item['intro'] = item['intro'][:999]
                if len(item['location']) > 3000:
                    item['location'] = item['location'][:2999]
                if len(item['address']) > 1000:
                    item['address'] = item['address'][:999]
                if len(item['feature']) > 100:
                    item['feature'] = item['feature'][:99]
                if not item['teacher_num']:
                    item['teacher_num'] = 0
                sql = """
                INSERT INTO school (id,name,city,intro,address,pic,course_pic,tel,feature,tags,average_price,location,teacher_num)
                VALUES ( '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%d')
                """
                data = (id,item['name'], item['city'], item['intro'], item['address'], item['pic'], item['course_pic'],
                        item['tel'], item['feature'], item['tags'], item['average_price'], item['location'],
                        item['teacher_num'])
                try:
                    cursor.execute(sql, data)
                # 捕获由于sql语句错误抛出的异常
                except pymysql.err.ProgrammingError:
                    date = time.strftime('%Y-%m-%d-%H%M%S', time.localtime(time.time()))
                    # 收集错误 redis key中元素对应索引和报错时间
                    err = (index, date)
                    l1.apend(err)
                # 捕获主键冲突
                except pymysql.err.IntegrityError:
                    date = time.strftime('%Y-%m-%d-%H%M%S', time.localtime(time.time()))
                    err = (index, date)
                    l2.apend(err)
                else:
                    connect.commit()
                    id += 1
                finally:
                    index += 1
                cursor.execute(sql % data)
                connect.commit()
    cursor.close()
    connect.close()
    date = time.strftime('%Y-%m-%d', time.localtime(time.time()))
    file1 = 'D:/jiaotou/course/mysql_err/sqlerr/%s.txt' % date
    file2 = 'D:/jiaotou/course/mysql_err/integrityerr/%s.txt' % date
    if l1:
        with open(file1, 'w') as f:
            f.write(str(l1))
    if l2:
        with open(file2, 'w') as f:
            f.write(str(l2))
