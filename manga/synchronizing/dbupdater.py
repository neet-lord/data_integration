# This script will read the mangalists
import django
import django.db.utils
import os
import subprocess
import sys
import datetime
import json
import scrapy

from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from scrapy.crawler import CrawlerProcess
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging

from prepenv import MANGA_SPIDERS_ROOT, MANGA_SPIDERS_DATA, DJANGO_APP_ROOT

from spiders.manga.global_settings import SPIDERS_JSON_FILE_PATHS
from manga.models import Manga, Chapter, Author, Tag, TagType, AlternateName, ExternalResource

OUTPUT_FOLDER = None
TMP_FOLDER = None

def prepare_workspaces():
    global OUTPUT_FOLDER
    OUTPUT_FOLDER = os.path.join(MANGA_SPIDERS_DATA, 'db_update_logs/')

    if not os.path.exists(OUTPUT_FOLDER):
        os.mkdir(OUTPUT_FOLDER)

    global TMP_FOLDER
    TMP_FOLDER = os.path.join(OUTPUT_FOLDER, '.tmp/')

    if not os.path.exists(TMP_FOLDER):
        os.mkdir(TMP_FOLDER)

def process_manga_json_file(json_file_path):
    process_params = prepare_processing_environment(json_file_path)

    log_file_path = process_params['log_file_name']
    lines_already_processed = process_params['logged_json_file_lines_processed']
    total_lines = process_params['logged_json_file_total_lines']
    json_lines = process_params['json_file_lines']

    if lines_already_processed == None or lines_already_processed < 0:
        lines_already_processed = 0

    lines_remaining = total_lines - lines_already_processed

    if lines_remaining == 0:
        print('[' + str(datetime.datetime.utcnow()) + '] According to the process logs, this file has been processed to completion. Modify the process logs if that isn\'t the case: ' + json_file_path)
        return

    print('[' + str(datetime.datetime.utcnow()) + '] Now processing: ' + json_file_path)

    current_line_index = lines_already_processed

    for x in range(lines_remaining):
        current_json_line = json_lines[current_line_index + x]
        progress_notif = '\t\t[CURRENT LINE]:' + str(current_line_index + x + 1) + '\r\n'
        
        # Only print a progress report to the console after 200 lines. It makes things faster.
        if x % 200 == 0:
            print(progress_notif)

        clean_json_line = clean_raw_json_line(current_json_line)
        manga_json_object = json.loads(clean_json_line)

        manga = save_manga_to_db(manga_json_object)

        get_manga_chapter_pages(manga)

        replace_line(log_file_path, 3, progress_notif)      

def prepare_processing_environment(json_file_path):
    process_instance = 'db_update_' + json_file_path.split('/')[-1]
    
    log_file_name = OUTPUT_FOLDER + process_instance
    log_file = None

    process_params = {
        'json_file_name': json_file_path,
        'json_file_total_lines': 0,
        'json_file_lines': list(),
        'log_file_name': log_file_name,
        'logged_json_file_lines_processed': None,
        'logged_json_file_total_lines': None,
        'logged_process_status': None,
    }

    logged_parameters = get_logged_process_parameters_if_they_exist(log_file_name)
    json_file_parameters = get_json_file_parameters(json_file_path)

    if logged_parameters == None:
        prepare_new_log_file(log_file_name)
        process_params['logged_json_file_total_lines'] = json_file_parameters['json_file_total_lines']
        replace_line(log_file_name, 2, '\t[LINES TO BE PROCESSED]:' + str(json_file_parameters['json_file_total_lines']) + '\r\n')
    else:
        process_params['logged_json_file_lines_processed'] = logged_parameters['logged_json_file_lines_processed']
        process_params['logged_json_file_total_lines'] = logged_parameters['logged_json_file_total_lines']
        process_params['logged_process_status'] = logged_parameters['logged_process_status']
        
    process_params['json_file_total_lines'] = json_file_parameters['json_file_total_lines']
    process_params['json_file_lines'] = json_file_parameters['json_file_lines']
    
    return process_params

def prepare_new_log_file(log_file_path):
    output_file = open(log_file_path, 'a+')
    
    timestamp = datetime.datetime.utcnow()

    output_message = '[PROCESS INITIATED]:' + str(timestamp) + '\r\n'
    output_message += '\t[SOURCE FILE]:' + json_file_path + '\r\n'
    output_message += '\t[LINES TO PROCESS]:0\r\n'
    output_message += '\t\t[CURRENT LINE]:0\r\n'

    print(output_message)

    output_file.write(output_message)
    output_file.close()

def get_json_file_parameters(json_file_path):
    json_file = open(json_file_path, 'r')
    lines = json_file.readlines()
    
    json_file_parameters = {
        'json_file_total_lines':len(lines),
        'json_file_lines': lines,
    }

    return json_file_parameters

def get_logged_process_parameters_if_they_exist(log_file_name):
    log_report = None
    
    # Check if there is a log for the process, so that we can resume
    # processing from the log, instead of processing from the beginning.
    if os.path.exists(log_file_name) and os.path.isfile(log_file_name):
        log_report = build_process_parameters_from_log(log_file_name)

    return log_report

def build_process_parameters_from_log(log_file_name):
    log_file = open(log_file_name, 'r')
    logged_lines = log_file.readlines()
    
    logged_line_count = 0

    progress_report = {
        'logged_process_status': 'The log file indicates that the JSON file is unprocessed.',
        'logged_json_file_lines_processed': None,
        'logged_json_file_total_lines': None,
    }

    if len(logged_lines) >= 3:
        for line in logged_lines:    
            line = line.split(':')

            for x in range(len(line)):
                line[x] = line[x].strip()

            if line[0] == '[CURRENT LINE]':
                report_line = line[1]
                progress_report['logged_json_file_lines_processed'] = int(report_line)    
            elif line[0] == '[LINES TO BE PROCESSED]':
                report_line = line[1]
                progress_report['logged_json_file_total_lines'] = int(report_line)
    
        if progress_report['logged_json_file_total_lines'] == None:
            # The file had the appropriate number of lines, but
            # there was no line indicating progress, signifying that
            # the log might be corrupt.
            progress_report['logged_json_file_lines_processed'] = -1
            progress_report['logged_json_file_total_lines'] = -1
        
        return progress_report

    # The file did not have the correct minimal number of lines,
    # therefore it will not even be treated as a log file.
    progress_report['logged_json_file_lines_processed'] = -2
    progress_report['logged_json_file_total_lines'] = -2
    progress_report['logged_process_status'] = 'The error log oes not have the correct number of lines.'

    return progress_report

def clean_raw_json_line(line):
    if line[0] == '"':
        line = line[1:]
        line = line[:-3]

        line = line.replace('\\\\', '\\')
        line = line.replace('\\"', '"')

    return line

def save_manga_to_db(manga_json):
    manga = Manga.objects.get_or_create(
        manga_name=manga_json["manga_name"]
    )

    manga = manga[0]
    
    manga.banner_image_url = manga_json["banner_image_url"]
    manga.description = manga_json["description"]

    try:
        manga.save()
    except django.db.utils.OperationalError:
        print("error found")
        raise

    authors = manga_json['manga_authors']
    
    if authors is not None:
        for x in range(len(authors)):
            author_json = authors[x]

            author = Author.objects.get_or_create(
                author_name=author_json['author_name'],
                author_status='Unknown',
                author_url=''
            )

            author = author[0]

            manga.manga_author.add(author)

    alternate_names = manga_json['alternate_names']

    if alternate_names is not None:
        for x in range(len(alternate_names)):
            alternate_name_json = alternate_names[x]
            alternate_name = AlternateName.objects.get_or_create(
                alternate_name=alternate_name_json["alternate_name"]
            )

            alternate_name = alternate_name[0]

            manga.alternate_names.add(alternate_name)

    tags = manga_json['tags']

    if tags is not None:
        for x in range(len(tags)):
            tag_json = tags[x]

            tag_type_object = TagType.objects.get_or_create(
                type_name=tag_json['tag_type']
            )

            tag_type_object = tag_type_object[0]

            tag = Tag.objects.get_or_create(
                tag_name=tag_json['tag_name'],
                tag_type=tag_type_object
            )

            tag = tag[0]

            manga.tags.add(tag)

    chapter_sources = manga_json['chapter_sources']

    if chapter_sources is not None:
        for x in range(len(chapter_sources)):
            resource_url = chapter_sources[x]

            resource_object = ExternalResource.objects.get_or_create(
                resource_type = 'Scrapable Web Page',
                url_expression = resource_url,
                resource_processing_script = 'MangakakalotChapterOverviewsSpider',
            )

            resource_object = resource_object[0]

            manga.chapter_sources.add(resource_object)

    chapter_items = manga_json['chapters']

    for chapter_item in chapter_items:

        chapter = ''
        try:
            chapter = Chapter.objects.get_or_create(        
                chapter_name=chapter_item['chapter_name'],
                chapter_number=chapter_item['chapter_number'],
                url=chapter_item['url'],
                manga=manga,
            )
        except:
            raise

        chapter = chapter[0]

        resource_items = chapter_item['external_sources']
        '''
        The future version of this code, after the next scraping:
        for resource_item in resource_items:
            resource_object = ExternalResource.objects.get_or_create(
                resource_type = resource_item['resource_type'],
                url_expression = resource_item['url_expression'],
                resource_processing_script = resource_item['resource_processing_script']
            )
        '''
        for resource_item in resource_items:
            resource_object = ExternalResource.objects.get_or_create(
                resource_type = 'Scrapable Web Page',
                url_expression = resource_item,
                resource_processing_script = 'MangakakalotChapterPagesSpider'
            )

            resource_object = resource_object[0]

            chapter.external_sources.add(resource_object)

    return manga      

def replace_line(file_name, line_num, text):
    _in = open(file_name, 'r')
    lines = _in.readlines()
    _in.close()

    lines[line_num] = text

    _out = open(file_name, 'w')
    _out.writelines(lines)
    _out.close()

def get_chapter_pages(chapter_id, chapter_url, processing_script):
    print("fetching chapter pages")
    print(MANGA_SPIDERS_ROOT)

    scrapy_project_dir = MANGA_SPIDERS_ROOT

    reactor_module = 'data_integration.manga.synchronizing.start_reactor'

    print("reactor: ")
    print(reactor_module)

    command = 'python3 -m {} -d "{}" -p "{}" -c "{}" -u "{}" -s "{}"'.format(
        reactor_module,
        DJANGO_APP_ROOT,
        scrapy_project_dir,
        chapter_id,
        chapter_url,
        processing_script
    )

    print(command)

    output = subprocess.Popen(
        [
            'python3',
            '-m', reactor_module,
            '-d', DJANGO_APP_ROOT,
            '-p', scrapy_project_dir,
            '-c', chapter_id,
            '-u', chapter_url,
            '-s', processing_script
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    stdout, stderr = output.communicate()
    
    print(stdout)
    print(stderr)

def get_manga_chapter_pages(manga):
    chapters = Chapter.objects.filter(manga_id=manga.id)

    thread_pool_executor = ThreadPool(2)
    
    for chapter in chapters:

        thread_pool_executor.apply_async(
            get_chapter_pages,
            (
                str(chapter.id),
                chapter.url,
                'MangakakalotChapterPagesSpider'
            )  
        )

    thread_pool_executor.close()
    thread_pool_executor.join()

print("Preparing environment")
prepare_workspaces()
os.chdir(MANGA_SPIDERS_ROOT)

print("Go!!!")
for json_file_path in SPIDERS_JSON_FILE_PATHS:
    process_manga_json_file(json_file_path)

#process_manga_json_file(args.input_file)
