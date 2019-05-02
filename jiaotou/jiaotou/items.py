# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JiaotouItem(scrapy.Item):
    # define the fields for your item here like:
    #字段依次为机构名、所在城市、简介、地址、机构图片路径、课程图片路径、电话、
    #特点、标签、课程均价、定位经纬度、区县、类型
    name = scrapy.Field()
    city = scrapy.Field()
    intro = scrapy.Field()
    address = scrapy.Field()
    pic = scrapy.Field()
    course_pic = scrapy.Field()
    tel = scrapy.Field()
    feature = scrapy.Field()
    tags = scrapy.Field()
    average_price = scrapy.Field()
    location = scrapy.Field()
    county = scrapy.Field()
    types = scrapy.Field()

class JiaotouCourseItem(scrapy.Item):
    # define the fields for your item here like:
    #字段包括课程名、机构名、类型、班级类型、招生数量、上课日期、
    #校区、状态、价格、课程介绍、面向学生
    course = scrapy.Field()
    name = scrapy.Field()
    type = scrapy.Field()
    class_type = scrapy.Field()
    student_num = scrapy.Field()
    date = scrapy.Field()
    campus = scrapy.Field()
    status = scrapy.Field()
    price = scrapy.Field()
    detail = scrapy.Field()
    client = scrapy.Field()
