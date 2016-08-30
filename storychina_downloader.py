import requests
from lxml import etree, html
from lxml.html.clean import clean_html
import re
import pathlib
import json
import sys, getopt
import threading
import time

def separate_sentences(paragraph):
    sentence_seps = list('。！？!?\n')

    old_sentences = [paragraph]
    new_sentences = []
    for sep in sentence_seps:
        # print('Sep: {}'.format(sep))
        for sentence in old_sentences:
            start = 0
            ix = sentence.find(sep, start)
            while(ix != -1):
                if ix+1 < len(sentence) and sentence[ix+1] in list('”\'"》'):
                    ix += 1
                extracted = sentence[start:ix+1].strip()
                if extracted != '':
                    new_sentences.append(extracted)
                start = ix+1
                ix = sentence.find(sep, start)

            if start < len(sentence):
                new_sentences.append(sentence[start:].strip())

        old_sentences = new_sentences.copy()
        new_sentences = []

    # print('Paragraph in: {}'.format(paragraph))
    # print('Sentences out: {}'.format(old_sentences))

    return old_sentences

def download_story(category, story_id):
    categories = [
        'BestStory',
        'PopAuthor',
        'OnlineStory'
    ]

    story_url = 'http://www.storychina.cn/frm{}_Detail.aspx'.format(categories[category])
    request_payload = {'ID': story_id}

    try:
        resp = requests.get(story_url, params=request_payload, allow_redirects=False)
    except:
        return None

    if 'Location' in resp.headers.keys():
        return None

    resp_text = resp.text
    text_start = resp_text.find('<div class="main_txt">')
    text_end = resp_text.find('<!-- Baidu Button BEGIN -->') - 326
    resp_text = resp_text[text_start:text_end] + '</div>'
    # print(resp_text)
    try:
        resp_html = html.document_fromstring(clean_html(resp_text))
    except:
        return None

    story = {}

    main_text = resp_html.find_class("main_txt")
    # print(main_text[0].text_content())
    if len(main_text) > 0:
        main_text = main_text[0]
        main_text = main_text.getchildren()
    else:
        return None

    title_element = main_text.pop(0)
    if title_element.tag == 'h1':
        story['title'] = title_element.text_content()

    attribution_element = main_text.pop(0)
    if attribution_element.tag == 'h2':
        attribution_text = attribution_element.text_content()
        attribution_text = attribution_text.replace('\xa0', ' ')
        if category == 0:
            try:
                m = re.match(r"(作者：)(?P<author>\S*(\s?\S+)*)(\s{2,})+.*(来源：)(?P<source>\S*(\s?\S+)*)(\s{2,}){1}.*(发布时间：)(?P<date>\S*(\s?\S+)*)", attribution_text)
                story['source'] = m.group('source')
            except:
                m = re.match(r"(作者：)(?P<author>\S*(\s?\S+)*)(\s{2,})+.*(发布时间：)(?P<date>\S*(\s?\S+)*)", attribution_text)
                story['source'] = None
        else:
            m = re.match(r"(作者：)(?P<author>\S*(\s?\S+)*)(\s{2,})+.*(发布时间：)(?P<date>\S*(\s?\S+)*)", attribution_text)
            story['source'] = None

        story['author'] = m.group('author')
        story['date'] = m.group('date')

    story['content'] = []
    for p in main_text:
        if p.tag in ['p', 'div']:
            paragraph_text = p \
                .text_content() \
                .replace('\xa0', ' ') \
                .strip()
            if paragraph_text == '':
                continue

            sentences = separate_sentences(paragraph_text)
            story['content'] += sentences

        # print(story['content'])

    if len(story['content']) == 0:
        attribution_end = resp_text.find('</h2>')+5
        body_text = resp_text[attribution_end:-6] \
            .replace('\xa0', ' ') \
            .replace('&nbsp;', ' ') \
            .replace('<BR>', ' ') \
            .replace('<br>', ' ') \
            .strip()
        sentences = separate_sentences(body_text)
        story['content'] = sentences

    return story

def process_story(story_category, story_id):
    story = download_story(story_category, story_id)

    if story is None:
        print('No story #{} in category {}'.format(story_id, story_category))
        # print(story_id, 'skip')
        return False

    common_data = {
        'title': story['title'],
        'author': story['author'],
        'source': story['source'],
        'date': story['date']
    }

    sentences = []
    for line in story['content']:
        sentence = common_data.copy()
        sentence['sentence'] = line
        sentences.append(sentence)

    story_file = pathlib.Path('stories/{} - {}.json'.format(story_category, story_id))
    with story_file.open('w', encoding='utf-8') as f:
        json.dump(sentences, f, ensure_ascii=False, indent=4)
        # print(story_id, '----')
        print('Downloaded story #{}-{}'.format(story_category, story_id))

    return True

def downloader(category, start, end, increment=1):
    start_time = time.monotonic()
    count, fail, streak = 0, 0, 0
    for i in range(start, end+1, increment):
        success = process_story(category, i)
        if success:
            count += 1
            streak = 0
        else:
            fail += 1
            streak += 1

        if streak > 1000:
            print('Ending thread, failure streak limit began at {}'.format(i - (streak*increment)))
            break

    duration = time.monotonic() - start_time
    print('Thread {}: success {} / false {} in {}s'.format(start, count, fail, duration))

if __name__ == '__main__':
    quit()
    stories_dir = pathlib.Path('stories')
    if not stories_dir.exists():
        stories_dir.mkdir()

    # process_story(0, 1)
    # process_story(0, 35)
    # quit()

    download_start = 1
    download_end = 1
    download_category = 0
    download_threads = 1

    opts, args = getopt.getopt(sys.argv[1:], "s:e:c:t:", ["start=", "end=", "cat=", "threads="])
    for o, a in opts:
        if o in ['-s', '--start']:
            download_start = int(a)
        elif o in ['-e', '--end']:
            download_end = int(a)
        elif o in ['-c', '--cat']:
            download_category = int(a)
        elif o in ['-t', '--threads']:
            download_threads = int(a)

    threads = []
    for i in range(download_start, download_start+download_threads):
        threads.append(threading.Thread(
            target=downloader,
            args=(download_category, i, download_end, download_threads)
        ))
        threads[-1].start()
        time.sleep(1)

    for thread in threads:
        thread.join()

    quit()
