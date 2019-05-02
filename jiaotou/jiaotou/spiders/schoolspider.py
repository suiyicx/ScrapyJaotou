#from scrapy.spiders import Rule#,CrawlSpider
from scrapy.selector import Selector
from scrapy.http import Request
#import re
import redis
#from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from jiaotou.items import JiaotouItem
from urllib.parse import urljoin
from urllib.request import urlretrieve
from scrapy_redis.spiders import RedisCrawlSpider
import os
import time
import random


class SchoolSpider(RedisCrawlSpider):
    name = 'jiaotou_school'
    allowed_domains = ["www.jiaotou.org"]
    #start_urls = ["https://movie.douban.com/top250"]
    redis_key = 'jiaotou:start_urls' #开始url在redis的键名
    #city = re.match(r'jiaotou_(\w+):.*', redis_key).group(1) #get city
    #图片存放路径
    path1 = 'D:/Upload/product/'
    path2 = '/Upload/product/'
    jiaotou_url = "http://www.jiaotou.org"
    #rules = [Rule(LxmlLinkExtractor(restrict_xpaths=('//dl[@class="clearfix"]/dt/p'),process_value="p_link"),
                  #callback="parse1",),
                 #Rule(LxmlLinkExtractor(restrict_xpaths=('//div[@class="paging-box2"]/ul[@id="pagerDiv"]/li[last()-1]'),
                                         #process_value="p_link"),
                                         # s.xpath('li[last()-1]/a/@href')
                  #callback="parse2",follow=True),
             #]

    #def p_link(self,link):

        #jiaotou_url = "http://www.jiaotou.org"
        #return urljoin(jiaotou_url,link)

    def parse(self, response):
        item = JiaotouItem()
        s = Selector(response)
        srcurls = s.xpath('//dl[@class="clearfix"]/dt')
        for srcurl in srcurls:
            #name1 = srcurl.xpath('p/a/text()').extract_first()
            src1 = srcurl.xpath('a/img/@src').extract_first()
            filepath2 = ''
            #下载课程图片以指定名称和路径保存到本地如F:\Upload\product\2018\2018.02\2018.02.06\15178827722806.png
            if src1:
                absolutesrc1 = urljoin(self.jiaotou_url, src1)
                #localtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                year = time.strftime('%Y', time.localtime(time.time()))
                month = time.strftime('%Y.%m', time.localtime(time.time()))
                day = time.strftime('%Y.%m.%d', time.localtime(time.time()))
                t = int(time.time())
                num = random.randint(1000, 9999)
                filename = str(t) + str(num) + '.png'
                filepath2 = self.path2 + year + '/' + month + '/' + day + '/' + filename
                fileYear = self.path1 + year
                fileMonth = fileYear + '/' + month
                fileDay = fileMonth + '/' + day
                filepath1 = fileDay + '/' + filename
                if not os.path.exists(fileYear):
                    os.mkdir(fileYear)
                    os.mkdir(fileMonth)
                    os.mkdir(fileDay)
                else:
                    if not os.path.exists(fileMonth):
                        os.mkdir(fileMonth)
                        os.mkdir(fileDay)
                    else:
                        if not os.path.exists(fileDay):
                            os.mkdir(fileDay)
                urlretrieve(absolutesrc1, filepath1)
            sch_url = srcurl.xpath('p/a/@href').extract_first()
            item['course_pic'] = filepath2
            city = s.xpath('//div[@class="citybox"]/a/text()').extract_first()
            item['city'] = city
            if sch_url:
                res = Request(urljoin(self.jiaotou_url, sch_url), callback=self.parse_school)
                res.meta['item'] = item
                yield res
        #提取下一页link继续爬取
        nextLink = s.xpath('//div[@class="paging-box2"]/ul[@id="pagerDiv"]/li[last()-1]/a/@href').extract_first()
        if nextLink:
            next = urljoin(self.jiaotou_url,nextLink)
            yield Request(next, callback=self.parse)

    def parse_school(self, response):
        item = response.meta['item']
        s = Selector(response)
        #把课程url存入redis以便后续爬取
        courses_url = urljoin(self.jiaotou_url, s.xpath(
            '//div[@class="clearfix nav-school"]/ul[@class="wrap"]/li[3]/a/@href').extract_first())
        pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
        r = redis.Redis(connection_pool=pool)
        pipe = r.pipeline(transaction=True)
        pipe.lpush("jt_course_urls", courses_url)
        pipe.execute()  # 保存学校页面的课程链接到reids
        msgs = s.xpath('//div[2]/div[1]/div[@class="div1"]')
        for msg in msgs:
            name = msg.xpath('p[@class="title"]/a/text()').extract_first()
            feature = msg.xpath('p[@class="title"]/span/text()').extract_first()
            tags = msg.xpath('div[@class="a1"]/a/text()').extract()
        item['feature'] = feature
        item['tags'] = tags
        item['name'] = name
        srcs = s.xpath('//div[@class="pic"]/div[@class="big"]/div/span/a/img/@src').extract()
        pic = []
        #最多爬取5张机构图片
        if srcs:
            i = 0
            for src in srcs:
                absolutesrc = urljoin(self.jiaotou_url, src)
                year = time.strftime('%Y', time.localtime(time.time()))
                month = time.strftime('%Y.%m', time.localtime(time.time()))
                day = time.strftime('%Y.%m.%d', time.localtime(time.time()))
                t = int(time.time())
                num = random.randint(1000, 9999)
                filename = str(t) + str(num) + '.png'
                filepath2 = self.path2 + year + '/' + month + '/' + day + '/' + filename
                fileYear = self.path1 + year
                fileMonth = fileYear + '/' + month
                fileDay = fileMonth + '/' + day
                filepath1 = fileDay + '/' + filename
                if not os.path.exists(fileYear):
                    os.mkdir(fileYear)
                    os.mkdir(fileMonth)
                    os.mkdir(fileDay)
                else:
                    if not os.path.exists(fileMonth):
                        os.mkdir(fileMonth)
                        os.mkdir(fileDay)
                    else:
                        if not os.path.exists(fileDay):
                            os.mkdir(fileDay)
                urlretrieve(absolutesrc, filepath1)
                pic.append(filepath2)
                i += 1
                if i == 5:
                    break
        item['pic'] = pic
        #item['city'] = self.city  # s.xpath('//title/text()').extract()
        #爬取机构简介并去除无用空格
        intros = s.xpath('//dl[@class="clearfix item-03"]/dd/div')
        intro1 = intros.xpath('p/text()').extract()
        intro = ''
        for text1 in intro1:
            if text1 != ' ':
                intro += text1.strip()
        if not intro:
            intro2 = intros.xpath('div/text()').extract()
            for text2 in intro2:
                if text2 != ' ':
                    intro += text2.strip()
        item['intro'] = ''.join(intro.split())
        msgs1 = s.xpath('//dl[@class="clearfix item-02"]')
        for msg1 in msgs1:
            tel = msg1.xpath('dt/span/text()').extract_first()
            addrs = msg1.xpath('dd/div/div/label/text()').extract()

            address = []
            for addr in addrs:
                if addr != ' ':
                    address.append(''.join(addr.strip().split()))

        item['address'] = address
        item['tel'] = tel
        item['average_price'] = s.xpath('//div[@class="info"]/div/span/em/b/text()').extract_first()
        url = s.xpath('//div[@class="clearfix nav-school"]/ul[@class="wrap"]/li[6]/a/@href').extract_first()
        if url:
            map_url = urljoin(self.jiaotou_url, url)
            res = Request(map_url, callback=self.parse_map)
            res.meta['item'] = item
            yield res
        else:
            item['location'] = None
            item['teacher_num'] = None

    def parse_map(self, response):
        item = response.meta['item']
        map_info = response.xpath('//div[5]/div/div/div/div[1]/dl[@class="Point"]')
        maps = []
        for info in map_info:
            map = []
            campus = info.xpath('dt/text()').extract_first()
            if campus != ' ':
                map1 = campus.strip()
            else:
                map1 = None
            campus_address = info.xpath('dd/p/text()').extract_first()
            if campus_address != ' ':
                 map2 = campus_address.strip()
            else:
                map2 = None
            value1 = info.xpath('dd/p/input[1]/@value').extract_first()
            value = tuple(value1.split(','))
            map.append(map1)
            map.append(map2)
            map.append(value)
            maps.append(map)
        item['location'] = maps
        urls = response.xpath('//div[3]/ul/li[5]/a/@href').extract_first()
        if urls:
            url = urljoin(self.jiaotou_url, urls)
            res = Request(url, callback=self.parse_teacher)
            res.meta['item'] = item
            yield res
        else:
            item['teacher_num'] = 0
            yield item

    def parse_teacher(self, response):
        item = response.meta['item']
        teachers = response.xpath('//div[5]/div/div[2]/div/div/dl')
        num = len(teachers)
        item['teacher_num'] = num
        yield item


