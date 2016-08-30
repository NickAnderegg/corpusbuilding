import requests
import json
import pathlib
import sys, getopt
import threading
import time

def create_mapping():
    index_mapping = '''
        {
            "settings": {
                "index": {
                    "analysis": {
                        "analyzer": {
                            "icu_analyzer": {
                                "tokenizer": "icu_tokenizer"
                            }
                        }
                    }
                }
            },
            "mappings": {
                "sentence": {
                    "properties": {
                        "sentence": {
                            "type": "string",
                            "fields": {
                                "smartcn": {
                                    "type": "string",
                                    "analyzer": "smartcn"
                                },
                                "icu": {
                                    "type": "string",
                                    "analyzer": "icu_analyzer"
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    '''
    # resp = requests.put('http://192.168.1.150:9200/sentences', data=index_mapping)
    # resp = requests.post('http://192.168.1.150:9200/sentences', data=settings_mapping)
    print(resp.text)

def prepare_document(story_category, story_id):
    story_file = pathlib.Path('stories/{} - {}.json'.format(story_category, story_id))
    if not story_file.exists():
        return 'skipped'

    exists = requests.head('http://192.168.1.150:9200/sentences/sentence/{}_{}_0'.format(story_category, story_id))
    if exists.status_code == 200:
        return 'already_exists'

    with story_file.open('r', encoding='utf-8') as f:
        story_content = json.load(f)

    bulk_string = []
    for ix, sentence in enumerate(story_content):
        create_string = {
            "create": {
                "_index": "sentences",
                "_type": "sentence",
                "_id": "{}_{}_{}".format(story_category, story_id, ix)
            }
        }
        bulk_string.append(json.dumps(create_string))
        bulk_string.append(json.dumps(sentence, ensure_ascii=False).replace('\n', ' '))

    bulk_string = bytearray('\n'.join(bulk_string), 'utf-8')
    try:
        resp = requests.post('http://192.168.1.150:9200/_bulk', data=bulk_string)
    except:
        return 'failed'
    # print(resp.text)
    # if resp.status_code != 200:
    #     print(resp.text)
    #     print(bulk_string)
    return 'submitted'

def inserter(category, start, end, increment=1):
    start_time = time.monotonic()
    count, fail, streak = 0, 0, 0
    for i in range(start, end+1, increment):
        success = prepare_document(category, i)
        if success == 'submitted':
            count += 1
            streak = 0
        else:
            fail += 1
            streak += 1

        if (fail+count) % 500 == 0:
            print('Thread {}: submitted {}'.format(start, count))

        # if streak > 1000:
        #     print('Ending thread, failure streak limit began at {}'.format(i - (streak*increment)))
        #     break

    duration = time.monotonic() - start_time
    print('Thread {}: success {} / false {} in {}s'.format(start, count, fail, duration))

if __name__ == '__main__':
    quit()

    insert_start = 1
    insert_end = 1
    insert_category = 0
    insert_threads = 1

    opts, args = getopt.getopt(sys.argv[1:], "s:e:c:t:", ["start=", "end=", "cat=", "threads="])
    for o, a in opts:
        if o in ['-s', '--start']:
            insert_start = int(a)
        elif o in ['-e', '--end']:
            insert_end = int(a)
        elif o in ['-c', '--cat']:
            insert_category = int(a)
        elif o in ['-t', '--threads']:
            insert_threads = int(a)

    threads = []
    for i in range(insert_start, insert_start+insert_threads):
        threads.append(threading.Thread(
            target=inserter,
            args=(insert_category, i, insert_end, insert_threads)
        ))
        threads[-1].start()
        time.sleep(1)

    for thread in threads:
        thread.join()

    quit()
