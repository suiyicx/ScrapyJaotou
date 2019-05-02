from scrapy.spider import CrawlSpider, Rule
import redis
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from urllib.parse import urljoin


class CitySpider(CrawlSpider):
    name = 'jiaotou_city'
    allowed_domains = ['jiaotou.org']
    #需要爬取的培训机构所在城市的列表
    start_urls = ['http://www.jiaotou.org/chengdu/s-list/', 'http://www.jiaotou.org/dazhou/s-list/',
                  'http://www.jiaotou.org/deyang/s-list/', 'http://www.jiaotou.org/luzhou/s-list/',
                  'http://www.jiaotou.org/meishan/s-list/', 'http://www.jiaotou.org/mianyang/s-list/']
    #额外选项：爬取时手动输入城市
    #def __init__(self,city=None,*args,**kwargs): #scrapy crawl cityspider -a city=shanghai
        #super(CitySpider,self).__init__(*args,**kwargs)
        #self.__city = city
        #self.start_urls = ['http://www.jiaotou.org/%s/s-list/'%city]
    #提取指定分类（语言培训、0-18岁、文体艺术）的link
    rules = [Rule(LxmlLinkExtractor(restrict_xpaths=('//div[@class="item"]/span[@class="node icon8"]')
                                    , process_value="pro_link"),callback="parse_city"),
             Rule(LxmlLinkExtractor(restrict_xpaths=('//div[@class="item"]/span[@class="node icon9"]')
                                    , process_value="pro_link"), callback="parse_city"),
             Rule(LxmlLinkExtractor(restrict_xpaths=('//div[@class="item"]/span[@class="node icon14"]')
                                    , process_value="pro_link"), callback="parse_city"),
             ]

    #处理LxmLinkExtractor提取的link
    def pro_link(self, link):
        jiaotou_url = "http://www.jiaotou.org"
        return urljoin(jiaotou_url, link)

    #解析url并存入redis数据库
    def parse_city(self, response):
        menus = 'jiaotou:start_urls' #%s_%s_%s_url'%(self.__city,menu1,menu2,menu3)
        #连接并存入redis数据库
        pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=1)
        r = redis.Redis(connection_pool=pool)
        pipe = r.pipeline(transaction=True)
        pipe.lpush(menus, response.url)
        pipe.execute()

