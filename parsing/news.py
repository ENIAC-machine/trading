import requests as rq
import polars as pl
import sys

from typing import Iterable, Iterator
from requests.models import DEFAULT_REDIRECT_LIMIT
from tqdm.auto import tqdm
from fake_useragent import UserAgent
from datetime import datetime, date, time
from time import sleep
from bs4 import BeautifulSoup


__all__ = ['scrape_moex_news']

#to parse news from
BASE_URL = "https://www.moex.com/{}/news/"


def _fetch_moex_news(st_date: datetime,
                     end_date: datetime,
                     fetch_article_content: bool = False,
                     lang: str = 'en',
                     verbose: bool = True,
                     timeout: int = 15
                     ) -> Iterator[list[date | time | str]]:

    gen_payload = lambda st_date, end_date, pge=1: {
        "day1": st_date,
        "day1mindate": st_date,
        "day1maxdate": "",
        "day2": end_date,
        "day2mindate": st_date,
        "day2maxdate": "",
        "exday1": st_date,
        "exday2": end_date,
        "exisdate": "0",
        "exncat": "200",
        "ncat": "200",
        'pge' : str(pge)
        }



    session = rq.Session()
    headers = {'User-Agent' : UserAgent().random,
               'Accept-Language' : f'{lang};q=0.9'
               }
    page = 1
    with tqdm(desc='Fetching data',
              disable=not verbose,
              leave=False) as pbar:

        while True:
            res = session.post(BASE_URL.format(lang),
                               data=gen_payload(st_date.strftime("%Y%m%d"),
                                                end_date.strftime("%Y%m%d"),
                                                pge=page),
                               headers=headers,
                               timeout=timeout)
            res.raise_for_status()

            soup = BeautifulSoup(res.text, 'html.parser')

            records = soup.find_all(class_='new-moex-news-list__record')
            
            if not records:
                if verbose:
                    print('All news collected')
                break

            for record in records:

                date_rec = record.find(class_='new-moex-news-list__date')
                time_rec = record.find(class_='new-moex-news-list__time')
                link_rec = record.find(class_='new-moex-news-list__link')
        
                article_date = date_rec.text.strip() if date_rec else ""
                article_time = time_rec.text.strip() if time_rec else ""

                title = link_rec.text.strip() if link_rec else ""
                href: str = link_rec['href'] if link_rec and link_rec.has_attr('href') else ""

                url = f'http://www.moex.com{href}' if href.startswith('/') else href

                if fetch_article_content:
                    response = session.get(url, headers=headers, timeout=timeout)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # The article title is usually in h1.news_title
                    title_art = soup.find('h1', class_='news_title')
                    title_art = title_art.text.strip() if title_art else ""
                    
                    # The article body is usually in div.news_text
                    content_art = soup.find('div', class_='news_text')
                    content_art = content_art.text.strip() if content_art else ""

                    yield [date.strptime(article_date, '%d.%m.%Y'),
                           time.strptime(article_time, '%H:%M'),
                           url, title_art, content_art]
                
                else:
                    yield [date.strptime(article_date, '%d.%m.%Y'),
                           time.strptime(article_time, '%H:%M'),
                           url]
            
            page += 1
            pbar.update(1)

            
def scrape_moex_news(st_date: datetime,
                     end_date: datetime,
                     fetch_article_content: bool = False,
                     out: str = 'df',
                     lang: str = 'en',
                     verbose: bool = True,
                     timeout: int = 15
                     ) -> pl.DataFrame | pl.LazyFrame:

    kwargs = locals()
    kwargs.pop('out', None)

    default_cols = [("Date", date),
                    ("Time", time),
                    ("Link", str),
                    ("Article Title", str),
                    ("Article Content", str)
                    ]

    cols = default_cols if fetch_article_content else default_cols[:-2]

    if out == 'df':
        return pl.DataFrame(data=_fetch_moex_news(**kwargs), schema=cols)

    elif out == 'lf':
        return pl.LazyFrame(data=_fetch_moex_news(**kwargs), schema=cols)

    else:
        raise ValueError('Wrong type of output requested, expected df or lf')


