
from jiaotou.items import JiaotouCourseItem
from scrapy.selector import Selector
from scrapy_redis.spiders import RedisCrawlSpider
import redis
import re
from urllib.parse import urljoin
from scrapy.http import Request
from w3lib.html import remove_tags


class CourseSpider(RedisCrawlSpider):
    name = 'jiaotou_course'
    allowed_domains = ['jiaotou.org']
    redis_key = 'jt_course_urls'
    jiaotou_url = "http://www.jiaotou.org"

    def parse(self, response):
        s = Selector(response)
        item = JiaotouCourseItem()
        name = s.xpath('//div[2]/div[1]/div[@class="div1"]/p[@class="title"]/a/text()').extract_first()
        infos = s.xpath('//div[5]/div/div[2]/div/div[2]/dl')
        #for i in range(len(infos)):
        for info in infos:
            info1 = info.xpath('dd/div[1]')
            course = info1.xpath('p/a/text()').extract_first()
            status = info1.xpath('p/span/text()').extract_first()
            #type1 = info1.xpath('ul/li[1]/a[1]/text()').extract_first()
            type2 = info1.xpath('ul/li[1]/a[2]/text()').extract()
            type3 = info1.xpath('ul/li[1]/a[3]/text()').extract()
            type = type2 + type3
            class_type = info1.xpath('ul/li[2]/i/text()').extract()
            student_num = info1.xpath('ul/li[3]/i/text()').extract_first()
            if student_num:
                student_num1 = student_num.strip()
            else:
                student_num1 = None
            times = info1.xpath('ul/li[4]/text()').extract()
            time2 = ''
            for time1 in times:
                if time1:
                    time2 += time1.strip()
            hour = '请咨询'
            time = []
            time.append(time2)
            time.append(hour)
            item['date'] = time
            campus1 = info1.xpath('ul/li[5]/div/div/i/a/text()').extract()
            campus = ','.join(campus1)
            price = infos.xpath('dd/div[2]/span/a/text()').extract_first()
            if price:
                price1 = price.strip()
            else:
                price1 = None
            item['name'] = name
            item['course'] = course
            item['status'] = status
            item['type'] = type
            #item['type2'] = type2
            #item['type3'] = type3
            item['class_type'] = class_type
            item['student_num'] = student_num1
            item['campus'] = campus
            item['price'] = price1
            url1 = info.xpath('dt/a/@href').extract_first()
            if url1:
                url = urljoin(self.jiaotou_url, url1)
                res = Request(url, callback=self.parse_detail)
                res.meta['item'] = item
                yield res
            else:
                item['detail'] = None
                item['client'] = None
                yield item
        pages = s.xpath('//div[5]/div/div[2]/div/div[2]/div/ul')
        page = pages.xpath('li[@class="curr"]/text()').extract_first()
        if page == '1':
            pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
            r = redis.Redis(connection_pool=pool)
            pipe = r.pipeline(transaction=True)
            urls = pages.xpath('li/a/@href').extract()
            match = re.match(r'(/S-Course/\d+-\d+\?p=)(\d+)', urls[-1]).groups()
            m1 = match[0]
            max = int(match[1])
            i = 2
            while i <= max:
                url1 = m1 + str(i)
                url = urljoin(self.jiaotou_url, url1)
                pipe.lpush("course_urls", url)
                pipe.execute()  # 保存学校页面的课程链接到reids
                i += 1

    def parse_detail(self, response):
        item = response.meta['item']
        infos = response.xpath('//div[5]/div/div[2]/div')
        details = infos.xpath('dl[1]/dd').extract()[0]
        detail1 = remove_tags(details)
        detail = ''.join(detail1.split())
        item['detail'] = detail
        client = infos.xpath('div/div[2]/div[2]/ul/li[3]/i//text()').extract_first()
        item['client'] = client
        yield item




