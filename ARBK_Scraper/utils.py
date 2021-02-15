main_url = "https://arbk.rks-gov.net/"
HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Accept-Encoding': 'gzip,deflate,sdch',
    'Accept-Language': 'en-US,en;q=0.8',
    'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
    'Connection': 'keep-alive'
}


def chunks(lst, n):
    """
    Yields n successive chunks.
    :param lst: list.
    :param n: number of chunks.
    """
    chunk_size = int(1.0*len(lst)/n)+1
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]
