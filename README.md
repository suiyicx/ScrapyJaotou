ScrapyJaotou
基于python3的教头网培训机构信息及其课程信息的Scrapy爬虫，实现分布式爬虫、图片下载和信息入库。
使用的工具:mysql、redis、scrapy、python3等。
流程：首先确定爬取哪个城市的培训机构作为start_url --> 提取指定分类链接并存入redis数据库 --> 读取redis数据库的分类链接开始爬取机构信息并将其课程链接存入redis数据库 --> 从redis数据库中获取课程链接并爬取课程信息 --> 最后将机构信息和课程信息保存到mysql数据库。在这个过程中，多个主机与一个redis数据库建立连接，读取链接并存入爬取数据，从而实现分布式爬虫，提高效率。
使用方法：
   #配置好redis和mysql数据库
   #进入jiaotou目录
   $cd d:/ScrapyJaotou/jiaotou
   #抓取数据
   $scrapy crawl cityspider #提取指定分类链接并保存到redis数据库
   $scrapy crawl schoolspider #爬取机构信息并保存课程链接到redis数据库
   $scrapy crawl coursespider #爬取课程信息
   #保存数据到mysql
   $cd d:/ScrapyJaotou/mysql #进入mysql目录
   $ python save.py #运行save.py执行保存操作
   



