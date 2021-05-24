import time

import requests
from lxml import etree
from elasticsearch_dsl import Date, Nested, Boolean, analyzer, Completion, Keyword, Text, Integer
from elasticsearch_dsl.connections import connections

connections.create_connection(hosts=['127.0.0.1'])
from elasticsearch import Elasticsearch

es = Elasticsearch()


def crawer_jokes(url):
    # 我们要抓取的网页起始地址
    # 用requests库的get方法下载网页
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
                             'Chrome/51.0.2704.63 Safari/537.36'}
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    r = res.text
    if res.status_code != 200:
        res = requests.get(url)
        r = res.text

    if res.status_code != 200:
        res = requests.get(url)
        r = res.text

    if res.status_code != 200:
        print(res.text)
    print(res.status_code)

    # 解析网页并且定位笑话内容
    s = etree.HTML(r)

    title = s.xpath("//div[@class='wrapper clearfix']/div[@class='main']/div[@class='section article']/div["
                    "@class='article-header']/h1")
    content = s.xpath("//div[@class='wrapper clearfix']/div[@class='main']/div[@class='section article']/div["
                      "@class='article-text']")
    next = s.xpath("//div[@class='wrapper clearfix']/div[@class='main']/div[@class='section article']/div["
                   "@class='article-header']/div[@class='article-page clearfix']/a[@class='next']/@href")[0]
    prev = s.xpath("//div[@class='wrapper clearfix']/div[@class='main']/div[@class='section article']/div["
                   "@class='article-header']/div[@class='article-page clearfix']/a[@class='prev']/@href")[0]

    contentString = ""
    for i in content:
        contentString += " " + i.xpath('string(.)')

    titleString = ""
    for i in title:
        titleString = i.text
        break

    # 打印抓取的信息
    return titleString, contentString, next, prev


def save_joke(title, content, realurl, time):
    doc = {
        'title': title,
        'content': content,
        'real_url': realurl,
        'create_time': time,
    }

    es.index(index='jokes', body=doc, id=realurl)


def save_news(title, content, realurl, time):
    doc = {
        'title': title,
        'content': content,
        'real_url': realurl,
        'create_time': time,
    }

    es.index(index='news', body=doc, id=realurl)


def crawler_jokes(count):
    url = "https://xiaohua.zol.com.cn"
    first = "/detail59/58238.html"
    title, content, next, prev = crawer_jokes(url + first)
    print('title =', title)
    print('content =', content)

    # count为需要抓取的数量
    for i in range(count):
        print("i =", i + 1)
        realurl = url + next
        print("url =", realurl)
        title, content, next, prev = crawer_jokes(realurl)
        print('title =', title)
        print('content =', content)
        now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(now_time)
        save_joke(title, content, realurl, now_time)
        time.sleep(0.1)


def crawer_news(url):
    # 我们要抓取的网页起始地址
    # 用requests库的get方法下载网页
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
                             'Chrome/51.0.2704.63 Safari/537.36'}
    res = requests.get(url, headers=headers)
    res.encoding = 'utf-8'
    r = res.text
    if res.status_code != 200:
        res = requests.get(url)
        r = res.text

    if res.status_code != 200:
        res = requests.get(url)
        r = res.text

    if res.status_code != 200:
        print(res.text)
    print(res.status_code)

    # 解析网页并且定位笑话内容
    s = etree.HTML(r)

    title = s.xpath("//div[@class='main index-main']/div[@class='panel-box']/div[@class='col-main']/div["
                    "@class='art-content']/div[@class='art-header']/h1")

    content = s.xpath("//div[@class='main index-main']/div[@class='panel-box']/div[@class='col-main']/div["
                      "@class='art-content']/div[@class='art-body']")

    contentString = ""
    for i in content:
        contentString += " " + i.xpath('string(.)')

    titleString = ""
    for i in title:
        titleString = i.text
        break

    return titleString, contentString


def crawler_news(count):
    urlhead = 'https://www.xinwentoutiao.net/xinxianshi/'
    urltail = '.html'
    first = 2247591
    for i in range(count):
        num = '%d' % first
        first -= 1
        real_url = urlhead + num + urltail
        title, content = crawer_news(real_url)
        print("url =", real_url)
        print('title =', title)
        print('content =', content)
        now_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(now_time)
        save_news(title, content, real_url, now_time)
        time.sleep(0.1)

if __name__ == '__main__':
    crawler_news(1000)
