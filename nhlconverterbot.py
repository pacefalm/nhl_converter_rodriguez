import tweepy
import sys
import configparser
import asyncio
from logging.handlers import RotatingFileHandler
from asyncio import Queue
from html.parser import HTMLParser
import re
import requests
import logging
import praw
import json
import time
import sqlite3
import redis
import os

UPDATE_INTERVAL = 30 # reddit rate limits to 1 request ever 2 seconds
CONFIG_PATH = "~/.config/converter.ini"

class NHLConverterBot(object):

    def __init__(self):
        self.update_interval = UPDATE_INTERVAL
        self.subreddit_names = [
           'pacefalmd', # testing
           'hockey',
           # pacific
           'anaheimducks',
           'coyotes',
           'calgaryflames',
           'edmontonoilers',
           'losangeleskings',
           'sanjosesharks',
           'canucks',
           # central
           #'hawks',
           'coloradoavalanche',
           'dallasstars',
           'wildhockey',
           'predators',
           'stlouisblues',
           'winnipegjets',
           # flortheast
           'bostonbruins',
           'sabres',
           'detroitredwings',
           'floridapanthers',
           'habs',
           'ottawasenators',
           'tampabaylightning',
           'leafs',
           # metro
           'canes',
           'bluejackets',
           'devils',
           'newyorkislanders',
           'rangers',
           'flyers',
           'penguins',
           'caps',
           # Extra
           'HalifaxMooseheads',
           ]
        #self.subreddit_names = ['pacefalmd']
        config = configparser.ConfigParser()
        config.read(os.path.expanduser(CONFIG_PATH))
        converter_config = config["hockey_converter_misc"]
        self.reddit = praw.Reddit(
            'NHL video converter',
            user_agent="linux:io.pacefalm.converter_bot:1.0.4 (by /u/pacefalmd)"
        )
        self.setup_redis()
        self.setup_twitter(converter_config)
        self.setup_logging(converter_config)
        self.setup_streamable(converter_config)

        self.fline = """**Mirrors/Alternate Angles**

[Direct link]({link_1800k})


^^^issues? ^^^contact ^^^/u/pacefalmd"""
        self.tline = """**Mirrors/Alternate Angles**

[Streamable link]({link_1800k})


^^^issues? ^^^contact ^^^/u/pacefalmd"""
        self.sline = """**Mirrors/Alternate Angles**


^^^issues? ^^^contact ^^^/u/pacefalmd"""
        self.last_updated = time.time()
        self.parser = HTMLParser()

    def setup_redis(self):
        self.redis = redis.StrictRedis()
        self.comment_queue = Queue(maxsize=1024)

    def setup_twitter(self, converter_config):
        consumer_key = converter_config["twitter_consumer_key"]
        consumer_secret = converter_config["twitter_consumer_secret"]
        access_token = converter_config["twitter_access_token"]
        access_token_secret = converter_config["twitter_access_token_secret"]
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.twitter = api = tweepy.API(auth)

    def setup_logging(self, converter_config):
        log_format = '%(asctime)-15s: %(message)s'
        logging.basicConfig(format=log_format)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        rh = RotatingFileHandler(converter_config["logging_path"])
        rh.setFormatter(formatter)
        self.logger.addHandler(rh)
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

    def setup_streamable(self, converter_config):
        self.streamable_auth = requests.auth.HTTPBasicAuth(
            converter_config["streamable_username"],
            converter_config["streamable_password"]
        )
        self.streamable_headers = {
                "User-Agent": f"linux:io.pacefalmd.converter (by {converter_config['streamable_username']})"
                }

    async def get_submissions(self):
        self.logger.info("Getting submissions")
        for subreddit in self.subreddit_names:
            try:
                sr = self.reddit.subreddit(subreddit)
                self.logger.debug("Getting submissions from subreddit {}".format(sr.display_name))
                for submissions in sr.new():
                    await self.filter_submissions(sr, submissions)
            except KeyboardInterrupt:
                sys.exit(1)
            except:
                self.logger.exception("failed to grab submissions")
                pass

    async def filter_submissions(self, sr, sub):
        self.logger.debug("Filtering submissions from sub {}".format(sr.display_name))
        res = {}
        if sub.is_self:
            return
        # Make sure we're not doing the same one over and over
        if not self.check_db(sub.id):
            self.logger.debug("Skipping duplicate: {}".format(sub.url))
            return
        # NHL.com videos
        if re.search('nhl.com(/[a-zA-Z].*)?/video/', sub.url):
            self.logger.info("Parsing nhl video link: {}".format(sub.url))
            asyncio.ensure_future(self.process_nhl_videos(sub, sub.url))
        if 'twitter.com' == sub.domain:
            self.logger.info("Parsing twitter link: {}".format(sub.url))
            await self.process_twitter(sub, sub.url)
        if 'streamable.com' == sub.domain:
            await self.process_streamable(sub)

    async def process_streamable(self, sub):
        self.logger.info("Responding to streamable link:\n\t{}".format(sub.id))
        asyncio.ensure_future(
            self.respond(sub, self.sline)

        )

    async def get_media(self, link):
        resp = requests.get(link)
        if resp.status_code == 200:
            return resp
        else:
            return []

    async def process_twitter(self, sub, link):
        link_list = link.split("/")
        tweet_id = None
        for idx, elem in enumerate(link_list):
            if elem == 'status':
                tweet_id = link_list[idx + 1]
                break
        if tweet_id == None:
            self.logger.info("Bad link: {}".format(link))
        video_status = False
        try:
            tweet = self.twitter.get_status(tweet_id, tweet_mode='extended')
            if not hasattr(tweet, "extended_entities"):
                self.logger.info("Link: {}\n\t has no extended entities".format(link))
                self.update_db(sub.id)
                return
            media = tweet.extended_entities.get("media", [])
            for _med in media:
                video_status = True if _med["type"] == "video" else False
                break
        except:
            self.logger.exception("Couldn't process tweet {tweet}".format(tweet=tweet_id))
            self.update_db(sub.id)
            return


        try:
            resp = requests.get(
                "https://api.streamable.com/import",
                auth=self.streamable_auth,
                headers=self.streamable_headers,
                params={"url": link})
            if resp.status_code == 504:
                self.logger.warning(
                    f"Streamable 504, Couldn't upload tweet {link}."
                    " Retrying..."
                )
                return
            response_json = resp.json()
            response = ""

        except:
            self.logger.exception(
                "Couldn't submit to streamable {tweet}\n"
                "\tStatus: {status}\n"
                "\ttext: {text}".format(
                tweet=tweet,
                status=resp.status_code,
                text=resp.text
            ))
            self.update_db(sub.id)
            return

        text = tweet.full_text.replace("\n\n", "\n").replace("\n", ". ")

        self.logger.info("Responding to twitter link:\n\t{}".format(tweet_id))
        asyncio.ensure_future(
            self.respond(sub,
            self.fline.format(
                title=text,
                link_1800k="https://streamable.com/{}".format(
                    response_json["shortcode"]
                    )
                )
            )
        )


    async def process_nhl_videos(self, sub, link):
        lines = await self.get_media(link)
        for line in lines.iter_lines():
            if b'var initialMedia' in line:
                try:
                    x = str(line)
                    y = ''.join(x.split('= ')[1].split(';')[0:-1])
                    y = y.replace('\\', '')
                    #self.logger.info(y)
                    z = json.loads(y)
                    asyncio.ensure_future(
                        self.construct_nhl_response(
                        sub,
                        z['metaData']['title'],
                            [a for a in z['metaData']['playbacks']]))
                except:
                    self.update_db(sub.id)
                    self.logger.info("Issue with %s", sub.url)

    async def construct_nhl_response(self, sub, title, playbacks):
        link_1800k = ''
        for a in playbacks:
            if a['name'] == 'FLASH_1800K_960X540':
                link_1800k = a['url']
        self.logger.info("Responding to nhl video link:\n\t{}".format(title))
        asyncio.ensure_future(
            self.respond(sub,
            self.fline.format(
                title=self.parser.unescape(title),
                link_1800k=link_1800k)))

    async def respond(self, sub, response):
        try:
            if not self.check_db(sub.id):
                return
            comment = sub.reply(response)
            if str(sub.subreddit) in ["pacefalmd", "hockey"]:
                comment.mod.distinguish(sticky=True)
        except praw.exceptions.APIException:
            self.logger.info("too old")
            pass
        finally:
            self.update_db(sub.id)

    def id_string(self, sub_id):
        return "async-convertor_bot-{}".format(sub_id)

    def check_db(self, id):
        if self.redis.exists(self.id_string(id)):
            return False
        return True

    def update_db(self, id):
        self.redis.setnx(self.id_string(id), True)

    def main(self):
        try:
            yield from asyncio.ensure_future(self.get_submissions())
            yield from asyncio.sleep(10)
        except KeyboardInterrupt:
            sys.exit(1)
        except Exception as e:
            self.logger.exception("Exception")
            pass


x = NHLConverterBot()
now = time.time
sleep_until = UPDATE_INTERVAL + now()
loop = asyncio.get_event_loop()
while True:
    loop.run_until_complete(x.main())
