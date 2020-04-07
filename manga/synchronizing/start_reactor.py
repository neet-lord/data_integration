import os
import sys
import argparse
import json
import django
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from scrapy.crawler import CrawlerProcess

parser = argparse.ArgumentParser()

parser.add_argument('--django_app_root', '-d', help="The root directory of the django project.", type=str)
parser.add_argument('--project_dir', '-p', help="Spiders project directory.", type= str)
parser.add_argument('--chapter_id', '-c', help="The id of the chapter in the database to which these pages should belong.", type= str)
parser.add_argument('--chapter_url', '-u', help="The url of the chapter from where the scraping should begin.", type= str)
parser.add_argument('--spider_script', '-s', help="The script to that will process the JSON file.", type=str)

print ('Parsing arguments for fetching chapter pages for chapter: ')
args = parser.parse_args()
print('Parsing successful. Attempting to pull chapter: ' + args.chapter_url +
        '\nUsing script: ' + args.spider_script)

def start_scraping_process():
    global args
    os.chdir(args.project_dir)

    process = CrawlerProcess(get_project_settings())

    process.crawl(
        args.spider_script,
        chapter_id=args.chapter_id,
        chapter_url=args.chapter_url,
    )

    process.start()

    return True

def verify_args():
    global args

    try:
        spider_script = args.spider_script
        chapter_url = args.chapter_url
        chapter_id = args.chapter_id
        project_dir = args.project_dir
        
        return True

    except NameError:
        print(parser.format_help())
        return False
    except:
        raise
args_present = verify_args()

if args_present:
    start_scraping_process()
