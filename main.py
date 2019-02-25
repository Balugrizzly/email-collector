import requests
import bs4 as bs
from urllib.parse import urlsplit
import threading
import sqlite3
import queue


"""
Style Guide

"""

parser = "lxml"

root_urls = [
             "https://www.youtube.com/",
             "https://en.wikipedia.org/wiki/Main_Page",
             "https://es.yahoo.com/",
             "https://www.reddit.com/"
             "https://github.com/"]


def get_base_url(url):
    return urlsplit(url)[0] + "://" + urlsplit(url)[1]


def get_sauce(url):
    try:
        sauce = requests.get(url, timeout=15.001).text
        sauces.put([sauce, url])
    except:
        pass


def get_links(sauce, sauce_url):
    # gets all the links from the sauce

    soupe = bs.BeautifulSoup(sauce, parser)
    base_url = get_base_url(sauce_url)

    for raw_link in soupe.find_all('a'):
        link = raw_link.get("href")

        # invalid links
        if link == None:
            continue

        if link == "javascript:void(0)":
            continue

        # full links
        if link.startswith("http://") or link.startswith("https://"):
            links.put(link)
            continue

        # on site links
        if link.startswith("/") or link.startswith("?"):
            links.put(base_url + link)
            continue

        # on site links with missing "/"
        links.put(base_url + "/" + link)


def get_mails(sauce):

    soupe = bs.BeautifulSoup(sauce, parser)

    for mail in soupe.find_all("a"):
        mail = mail.get("href")

        if mail == None:
            continue

        if mail.startswith("?"):
            continue

        if "mailto:" in mail:
            # skip strange amazon mails
            if mail[7:].startswith("?"):
                continue

            mails.put(mail[7:])


def db_manager():

    db = sqlite3.connect('collector.db')
    cursor = db.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sauces(id INTEGER PRIMARY KEY, sauce TEXT, base_url TEXT)
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS links(id INTEGER PRIMARY KEY, link TEXT, visited INT)
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS mails(id INTEGER PRIMARY KEY, mail TEXT)
    ''')
    db.commit()

    while True:

        # # sauces
        # while not db_sauces.empty():
        #     raw_sauce = sauces.get()
        #     sauce = raw_sauce[0]
        #     url = raw_sauce[1]
        #     base_url = get_base_url(url)
        #
        #     cursor.execute('''INSERT INTO sauces(sauce, base_url)
        #                       VALUES(?,?)''', (sauce, base_url))
        #
        #     cursor.execute('''UPDATE links SET visited = ? WHERE link = ? ''',
        #                    (time.time(), url))
        #     db.commit()

        while not mails.empty():
            mail = mails.get()

            cursor.execute('''INSERT INTO mails(mail)
                              VALUES(?)''', (mail,))
            db.commit()

        # while not db_links.empty():
        #     link = links.get()
        #     cursor.execute('''INSERT INTO links(link)
        #                       VALUES(?)''', (link,))
        #     db.commit()


threads = []
sauces = queue.Queue()
mails = queue.Queue()
links = queue.Queue()

dbm = threading.Thread(target=db_manager)
dbm.start()

# Getting our initial sauces and putting them in the sauces queue
for link in root_urls:
    t = threading.Thread(target=get_sauce, args=(link,))
    t.start()
    threads.append(t)

for t in threads:
    t.join()


while True:
     # redeclaring list
    threads = []

    # get links and mails from sauce
    print("start looping over the sauces in the sauces queue")
    while not sauces.empty():
        raw_sauce = sauces.get()
        sauce = raw_sauce[0]
        url = raw_sauce[1]
        base_url = get_base_url(url)

        print("scraping {}".format(url))

        get_links(sauce, base_url)
        get_mails(sauce)

    print("get more sauces")
    # get more sauces
    while not links.empty() and threading.active_count() < 10 and sauces.qsize() < 1000:
        link = links.get()
        print("getting sauce of {}".format(link))
        t = threading.Thread(target=get_sauce, args=(link,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if links.empty() and sauces.empty():
        print("we ran out of links and sauces")
        break

    print("loop finished")
    print("there are {} links and {} sauces left".format(
        links.qsize(), sauces.qsize()))
