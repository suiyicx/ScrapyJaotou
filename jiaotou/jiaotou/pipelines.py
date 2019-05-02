# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from twisted.enterprise import adbapi
import pymysql.cursors
from scrapy.crawler import Settings


class MysqlSchoolPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls,settings):
        dbparms = dict(
            host='127.0.0.1',
            db='haoke',
            user='root',
            passwd='123456',
            #port=settings('MYSQL_PORT'),
            charset='utf8',
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True
        )
        dbpool = adbapi.ConnectionPool("pymysql", dbparms)
        return cls(dbpool)

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error, item, spider)

    def do_insert(self, cursor, item):
        sql = """
            INSERT INTO school (name,city,intro,address,pic,course_pic,tel,feature,tags,average_price,
            location,teacher_num) VALUES ( '%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s',,'%d')
            """
        data = (item['name'],item['city'],item['intro'],item['address'],item['pic'],item['course_pic'],item['tel'],
                item['feature'],item['tags'],item['average_price'],item['location'],item['teacher_num'])
        cursor.execute(sql % data)

    def handle_error(self, failure, item, spider):
        print(failure)