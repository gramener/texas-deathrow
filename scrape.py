import os
import csv
import asyncio
import aiohttp
import logging
from hashlib import md5
from lxml.html import parse
from urllib.parse import urljoin
from urllib.error import HTTPError
from collections import OrderedDict


def getter(limit, cache=None, delay=0.1):
    '''
    Return a function that asynchronously gets a URL, up to `limit` in parallel,
    and saves the result in the `cache` folder as a binary file.

        >>> get = getter(10, cache='.cache', delay=0.1)
        >>> filename = get(url)
    '''
    get_total, get_count = 0, 0
    if cache is not None and not os.path.exists(cache):
        os.makedirs(cache)

    @asyncio.coroutine
    def get(url):
        '''Returns the lxml tree of a URL, loading fom a cache'''

        # We'll cache the file in .cache/<md5(URL)>
        path = os.path.join(cache, md5(url.encode('utf-8')).hexdigest())

        # If the URL is not already cached, get it and cache it.
        if not os.path.exists(path):
            # Never run more than limit GET requests at a time.
            # Keep releasing control and re-checking periodically until we're
            # below limit running GET requests.
            nonlocal get_count, get_total
            while get_count >= limit:
                yield from asyncio.sleep(delay)

            # Now, let's actually get the URL. get_count has the number of running
            # GET requests. get_total is the cumulative count of GET requests.
            get_count, get_total = get_count + 1, get_total + 1
            logging.info('Task# %d (total: %d). %s', get_count, get_total, url)
            response = yield from aiohttp.request('GET', url=url)
            get_count -= 1

            # Save the response fully (without decoding) into the cache
            if response.status == 200:
                result = yield from response.read_and_close()
                with open(path, 'wb') as handle:
                    handle.write(result)
            else:
                raise HTTPError(response.status)

        # By now, the URL is cached in path. Return the lxml tree
        return parse(path)

    return get


@asyncio.coroutine
def scrape(get):
    '''Scrape the Texas Death Row list and summarise into deathrow.csv'''

    # Get all the information you can from the main index page
    url = 'https://www.tdcj.state.tx.us/death_row/dr_executed_offenders.html'
    tree = yield from get(url)
    rows = []
    for row in tree.findall('.//tr')[1:]:
        cells = row.findall('td')
        rows.append(OrderedDict((
            ('id', cells[0].text),
            ('last_words_link', urljoin(url, cells[2].find('a').get('href'))),
            ('last_name', cells[3].text),
            ('first_name', cells[4].text),
            ('tdcj_number', cells[5].text),
            ('age', cells[6].text),
            ('date', cells[7].text),
            ('race', cells[8].text),
            ('county', cells[9].text),
        )))

    # Get all the last word links -- asynchronously. Up to limit requests
    # will be fired off without waiting for the response. As the responses
    # arrive, more requests will be fired. This line will block until all
    # responses are received.
    last_words = yield from asyncio.gather(*(
        get(row['last_words_link']) for row in rows))

    # Parse each last words page and add last words into rows['last_words']
    for tree, row in zip(last_words, rows):
        paras = tree.find('.//div[@id="body"]').findall('p')
        for i, para in enumerate(paras):
            if (para.get('class') == 'text_bold' and
                    para.text.lower().strip().startswith('last statement')):
                row['last_words'] = '\n'.join(para.text_content() or ''
                                              for para in paras[i + 1:])
                break

    # Save the file as CP-1252 encoding (which is what Excel uses)
    with open('deathrow.csv', 'w', encoding='cp1252') as handle:
        out = csv.DictWriter(handle, rows[0].keys(), lineterminator='\n')
        out.writeheader()
        out.writerows(rows)


# Log all information requests
logging.basicConfig(level=logging.INFO)

# Start the main event loop and run the scrape() function
loop = asyncio.get_event_loop()
loop.run_until_complete(scrape(get=getter(limit=2, cache='.cache')))
loop.close()
