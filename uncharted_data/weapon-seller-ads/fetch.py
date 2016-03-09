import json
import requests
import re
import time
from multiprocessing import Process, Queue


def query_solr(url, payload, auth, output_file, limit=1, rows=1000):
    """
    Queries SOLR for records using a cursor and writes each document as a json line to the output file.
    :param output_file: The file to write to
    :param limit: The maximum number of records to fetch
    :param rows: Number of records to return in each request
    :return:
    """
    payload['rows'] = 0
    payload['start'] = 0

    payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())

    r = requests.get(url, params=payload_str, auth=auth)

    if r.status_code is not 200:
        print "Received unexpected response from server:\n" + r.text

    json_data = r.json()

    total_count = json_data['response']['numFound']
    cursor_mark = '*'
    count = 0

    payload['rows'] = rows

    print "Total available document count is " + str(total_count) + "."

    with open(output_file, 'w') as o:

        print "Opened " + output_file + " for writing."

        while count < total_count and (limit < 0 or count < limit):

            payload['cursorMark'] = cursor_mark
            payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())

            r = requests.get(url, params=payload_str, auth=auth)

            if r.status_code is not 200:
                print "Received unexpected response from server:\n" + r.text

            json_data = r.json()
            docs = json_data['response']['docs']
            cursor_mark = json_data['nextCursorMark']

            for doc in docs:
                count += 1
                o.write(json.dumps(doc, sort_keys=True) + '\n')

                if 0 <= limit <= count:
                    break

                if count % 1000 == 0:
                    print "Writing record " + str(count) + "..."

    print "Writing of " + str(count) + " records complete."


def related_ad_links_by_user(auth, output_file='related-ad-links-raw.txt', limit=1, rows=1000):
    """
    Queries imagecat for records and writes each document as a json line to the output file.
    :param output_file:
    :param limit:
    :param rows:
    :return:
    """
    url='http://imagecat.dyndns.org/solr/imagecatdev/query'

    payload = {'q': 'id:/.*com%5C/armslist.*/',
               'fq': 'url:/.*relatedto.*/',
               'fl': 'url,id,outpaths,outlinks,content',
               'sort': 'id+asc'}

    query_solr(url, payload, auth, output_file, limit, rows)


def _identify_links(json_data, pattern):

    outlinks = json_data['outlinks']
    outpaths = json_data['outpaths']

    indices = []

    index = 0
    for link in outlinks:
        if pattern.match(link):
            indices.append(index)
        index += 1

    links = []

    for index in indices:
        links.append({'id': outpaths[index],
                      'url': outlinks[index],
                      'cdr_id': outpaths[index][-64:]})

    return links


def identify_post_links(input_file='related-ad-links-raw.txt', output_file='related-ad-links.txt'):
    """
    Processes each document to filter down to a group of imagecat ID and page URL for related ads, written as a JSON line.
    :param input_file:
    :param output_file:
    :return:
    """
    group_id = 0

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        with open(output_file, 'w') as o:
            print "Opened " + output_file + " for writing."

            pattern = re.compile('.*com/posts/[0-9]+/.*')

            count = 0
            ad_count = 0
            for line in i:
                count += 1
                try:
                    json_data = json.loads(line)

                    links = _identify_links(json_data, pattern)

                    if len(links) == 0:
                        continue

                    ad_count += len(links)

                    group = {'group': group_id,
                             'records': [],
                             'url': json_data['url'],
                             'id': json_data['id']}
                    group_id += 1

                    group['records'] = links

                    o.write(json.dumps(group, sort_keys=True) + '\n')

                    if count % 1000 == 0:
                        print "Writing record " + str(count) + "..."

                except Exception as e:
                    print str(e)

    print "Writing of " + str(ad_count) + " ads into " + str(group_id) + " group records complete."


def consolidate(input_file='related-ad-links.txt', output_file='related-ad-groups.txt'):
    """
    Uses an in-memory cache to consolidate groups with overlapping URLs.
    :param input_file:
    :param output_file:
    :return:
    """
    id_cache = {}
    group_cache = {}
    group_id = 0
    count = 0

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        for line in i:
            count += 1
            try:
                json_data = json.loads(line)
                group = None

                # Check if we've seen this URL already
                for record in json_data['records']:
                    if record['url'] in id_cache:
                        # We have, so get the existing group assignment
                        group = id_cache[record['url']]
                        break

                # If none of the URLs in this group have been seen before...
                if group is None:
                    group = group_id
                    group_id += 1

                # If we've identified an existing group for at least one URL in this set
                if group in group_cache:
                    # Add the source record for these URLs to the existing group
                    group_cache[group]['sources'].append({'id': json_data['id'],
                                                          'url': json_data['url'],
                                                          'cdr_id': json_data['id'][-64:]})

                    # Add URLs to the existing group
                    for record in json_data['records']:
                        # Make sure we're not adding duplicates
                        if record['url'] not in id_cache:
                            group_cache[group]['records'].append(record)

                # New group definition
                else:
                    group_cache[group] = {}
                    group_cache[group]['group'] = group
                    group_cache[group]['sources'] = [{'id': json_data['id'],
                                                      'url': json_data['url'],
                                                      'cdr_id': json_data['id'][-64:]}]
                    group_cache[group]['records'] = json_data['records']

                # Cache the newly processed URLs with the group they were assigned to
                for record in json_data['records']:
                        id_cache[record['url']] = group

            except Exception as e:
                print str(e)

    print "Consolidated " + str(len(id_cache)) + " unique ads from " + str(count) + " groups into " + str(len(group_cache)) + " groups."

    with open(output_file, 'w') as o:
        print "Opened " + output_file + " for writing."

        for group in range(0, group_id):
            o.write(json.dumps(group_cache[group], sort_keys=True) + '\n')

    print "Writing of " + str(group_id) + " group records complete."


def _fetch_ads(input_queue, output_queue, auth):

    url='http://imagecat.dyndns.org/solr/imagecatdev/query'

    payload = {'q': '*',
               'fq': '',
               'fl': 'id,content,outlinks,outpaths'}

    while True:
        json_string = input_queue.get()

        if json_string == 'exit':
            break
        else:
            try:
                json_data = json.loads(json_string)

                ids = []

                for ad in json_data['records']:
                    ids.append(ad['id'])

                payload['fq'] = 'id:("' + '" or "'.join(ids) + '")'
                payload_str = "&".join("%s=%s" % (k,v) for k,v in payload.items())

                r = requests.post(url, data=payload_str, auth=auth, headers={"Content-Type": "application/x-www-form-urlencoded"})

                if r.status_code is not 200:
                    print "Received unexpected response from server:\n" + r.text
                else:
                    response_json_data = r.json()
                    if response_json_data['response']['numFound'] > 0:
                        for doc in response_json_data['response']['docs']:
                            for ad in json_data['records']:
                                if ad['id'] == doc['id']:
                                    for key, value in doc.iteritems():
                                        ad[key] = value
                                    break

                for ad in json_data['records']:
                    if 'content' not in ad:
                        ad['content'] = ""
                    if 'outpaths' not in ad:
                        ad['outpaths'] = ""
                    if 'outlinks' not in ad:
                        ad['outlinks'] = ""

                output_queue.put(json.dumps(json_data, sort_keys=True) + '\n')

            except Exception as e:
                print str(e)


def fetch_ads(auth, input_file='related-ad-groups.txt', output_file='related-ads.txt', processes=8):

    if processes < 2:
        processes = 2

    count = 0

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        input_queue = Queue(100)
        output_queue = Queue(100)

        writer = Process(target=_write_file, args=(output_queue, output_file))
        writer.start()

        workers = []
        for index in range(0, processes - 1):
            worker = Process(target=_fetch_ads, args=(input_queue, output_queue, auth))
            worker.start()
            workers.append(worker)

        for line in i:
            count += 1
            input_queue.put(line)

        for index in range(0, processes - 1):
            input_queue.put('exit')

        for index in range(0, processes - 1):
            workers[index].join()

        output_queue.put('exit')
        writer.join()

    print "Writing of " + str(count) + " records complete."


def identify_image_links(input_file='related-ads.txt', output_file='related-ads-imgs.txt'):
    """
    Processes each document to filter down to a group of imagecat ID and page URL for related ads, written as a JSON line.
    :param input_file:
    :param output_file:
    :return:
    """

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        with open(output_file, 'w') as o:
            print "Opened " + output_file + " for writing."

            pattern = re.compile('http://\w+.armslist.com/sites/armslist/uploads/posts/.*')

            count = 0
            for line in i:
                count += 1
                try:
                    json_data = json.loads(line)

                    for ad in json_data['records']:

                        links = _identify_links(ad, pattern)
                        ad['images'] = links

                    o.write(json.dumps(json_data, sort_keys=True) + '\n')

                    if count % 1000 == 0:
                        print "Writing record " + str(count) + "..."

                except Exception as e:
                    print str(e)

    print "Writing of " + str(count) + " records complete."


def _term_search(input_queue, output_queue, terms_list, strip_content):

    while True:
        json_string = input_queue.get()

        if json_string == 'exit':
            break
        else:
            try:
                json_data = json.loads(json_string)
                group_terms = {}

                for ad in json_data['records']:
                    content = ad['content'].lower()

                    terms = {}
                    for term in terms_list:
                        matches = len(term[0].findall(content))
                        if matches > 0:
                            if term[1] in terms:
                                terms[term[1]] += matches
                            else:
                                terms[term[1]] = matches
                            if term[1] in group_terms:
                                group_terms[term[1]] += matches
                            else:
                                group_terms[term[1]] = matches

                    ad['terms'] = terms
                    ad['unique_terms'] = len(terms)
                    ad['total_terms'] = sum(terms.values())

                    if strip_content:
                        ad.pop('content', None)
                        ad.pop('outlinks', None)
                        ad.pop('outpaths', None)

                json_data['terms'] = group_terms
                json_data['unique_terms'] = len(group_terms)
                json_data['total_terms'] = sum(group_terms.values())

                output_queue.put(json.dumps(json_data, sort_keys=True) + '\n')

            except Exception as e:
                print str(e)


def _write_file(queue, output_file):

    with open(output_file, 'w') as o:
        print "Opened " + output_file + " for writing."

        count = 0
        while True:
            string = queue.get()
            if string == 'exit':
                break
            else:
                count += 1

                if count > 0 and count % 100 == 0:
                    print "Writing record " + str(count) + "..."
                    o.flush()

                o.write(string)


def apply_terms(terms_file='atf-keywords.txt', input_file='related-ads-imgs.txt', output_file='related-ads-terms.txt', strip_content=True, processes=8):

    if processes < 2:
        processes = 2

    terms_list = []

    with open(terms_file, 'r') as i:
        print "Opened " + terms_file + " for reading."

        unique = set()
        for line in i:
            line = line.rstrip('\r\n')
            if len(line) > 0:
                unique.add(line)

        for line in unique:
            try:
                terms_list.append((re.compile(r'\b({0}(s|\'s|es){{0,1}})\b'.format(line), flags=re.IGNORECASE), line))
            except Exception as e:
                print str(e) + ": " + line

        print "Loaded " + str(len(terms_list)) + " terms or phrases."

    count = 0

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        input_queue = Queue(100)
        output_queue = Queue(100)

        writer = Process(target=_write_file, args=(output_queue, output_file))
        writer.start()

        workers = []
        for index in range(0, processes - 1):
            worker = Process(target=_term_search, args=(input_queue, output_queue, terms_list, strip_content))
            worker.start()
            workers.append(worker)

        for line in i:
            count += 1
            input_queue.put(line)

        for index in range(0, processes - 1):
            input_queue.put('exit')

        for index in range(0, processes - 1):
            workers[index].join()

        output_queue.put('exit')
        writer.join()

    print "Writing of " + str(count) + " records complete."


def _fetch_cdr(input_queue, output_queue, auth):

    url='https://els.istresearch.com:19200/memex-domains_2016.02.03/weapons/_mget'

    while True:
        json_string = input_queue.get()

        if json_string == 'exit':
            break
        else:
            try:
                json_data = json.loads(json_string)

                seller = {'seller_uid': json_data['group'],
                          'unique_terms': json_data['unique_terms'],
                          'total_terms': json_data['total_terms'],
                          'terms': json_data['terms'],
                          'ad_images': []}
                ids = []

                for ad in json_data['records']:
                    ids.append({'_id': ad['cdr_id'], '_source': {'exclude': ['raw_content']}})
                    for image in ad['images']:
                        ids.append({'_id': image['cdr_id']})

                payload = {'docs': ids}

                r = requests.post(url, json=payload, auth=auth, headers={"Content-Type": "application/json"})

                if r.status_code is not 200:
                    print "Received unexpected response from server:\n" + r.text
                else:
                    response_json_data = r.json()
                    docs = response_json_data['docs']

                    # The ElasticSearch _mget request returns in the same order as requested, so we can just pop.
                    for ad in json_data['records']:
                        cdr = {'ad': docs.pop(0),
                               'unique_terms': ad['unique_terms'],
                               'total_terms': ad['total_terms'],
                               'terms': ad['terms'],
                               'images': []}

                        for image in ad['images']:
                            cdr['images'].append(docs.pop(0))

                        seller['ad_images'].append(cdr)

                    output_queue.put(json.dumps(seller, sort_keys=True) + '\n')

            except Exception as e:
                print str(e)


def to_cdr(auth, input_file='related-ads-terms.txt', output_file='related-ads-cdr.txt', processes=8):

    if processes < 2:
        processes = 2

    count = 0

    with open(input_file, 'r') as i:
        print "Opened " + input_file + " for reading."

        input_queue = Queue(100)
        output_queue = Queue(100)

        writer = Process(target=_write_file, args=(output_queue, output_file))
        writer.start()

        workers = []
        for index in range(0, processes - 1):
            worker = Process(target=_fetch_cdr, args=(input_queue, output_queue, auth))
            worker.start()
            workers.append(worker)

        for line in i:
            count += 1
            input_queue.put(line)

        for index in range(0, processes - 1):
            input_queue.put('exit')

        for index in range(0, processes - 1):
            workers[index].join()

        output_queue.put('exit')
        writer.join()

    print "Writing of " + str(count) + " records complete."
    pass

# Auth tuples are ('<username>', '<password>')
auth_imagecat = ('darpamemex', '')
auth_cdr = ('memex', '')

print "Start time " + time.strftime("%X")

related_ad_links_by_user(auth=auth_imagecat, limit=-1)
identify_post_links()
consolidate()
fetch_ads(auth=auth_imagecat, processes=16)
identify_image_links()
apply_terms(terms_file='atf-keywords.txt')
to_cdr(auth=auth_cdr, processes=16)

print "End time " + time.strftime("%X")

