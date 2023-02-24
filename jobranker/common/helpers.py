# Copyright Rex W, Douglass 2023
# SPDX-License-Identifier: Apache-2.0

"""
This provides a set of utilities for scraping pages and inserting their content into the database
"""

import pandas as pd

def load_google_sheet(sheet_id: str, sheet_name: str) -> pd.DataFrame:
  """
    Downloads a google sheet given the sheetid and sheet name and returns as a pandas dataframe.
    
    .. note:: Make sure the google doc is set to vieable with shared link

    :param sheet_id: The id of the google sheet taken from the URL
    :param sheet_name: The name of the specific sheet taken from the tab within the google sheet
    :type arg1: string
    :type arg2: string
    :returns: Data Frame
    :rtype: pandas.DataFrame

  """
  url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
  df = pd.read_csv(url)
  return(df)


import pandas as pd
import numpy as np
import asyncio
from pyppeteer import launch
import sys

def scrape_url(url: str, headless=False) -> str:
  """
    Opens a website URL using Chrome via puppeteer, waits until all of the content including javascript renders, and then returns the HTML of the page.
    
    .. note:: Some websites refuse headless mode and so the default is open the full browser.

    :param url: The URL of the website.
    :param headless: Whether to run chrome in headless mode.
    :type arg1: string
    :type arg2: Bool
    :returns: The website's HTML
    :rtype: string

  """
  
  #If URL doesn't start with https:// then add it
  if not url.startswith("http"):
    url= "https://" + url

  async def main(url, headless=False):

    browser = await launch({"headless": headless, #there are some pages that just refuse headless
    "args": [
      "--start-maximized" #, 
      #'--disable-extensions-except=/mnt/8tb_a/rwd_github_private/DataJobsByRexDouglass/plugins/ISDCAC-chrome-source/', 
      #'--load-extension=/mnt/8tb_a/rwd_github_private/DataJobsByRexDouglass/plugins/ISDCAC-chrome-source/'
      ]})
    # open a new tab in the browser
    page = await browser.newPage()
    await page.setViewport({"width": 1600, "height": 900})
    #https://stackoverflow.com/questions/59471331/how-to-disable-images-css-in-pyppeteer
    await page.setRequestInterception(True)
    async def intercept(request):
      if any(request.resourceType == _ for _ in ('stylesheet', 'image', 'font')):
          await request.abort()
      else:
          await request.continue_()
    page.on('request', lambda req: asyncio.ensure_future(intercept(req)))
    # add URL to a new page and then open it
    #
    await page.goto(url)
    # create a screenshot of the page and save it
    await page.waitFor(15000)
    #async def close_dialog(dialog):
    #  await dialog.dismiss()
    #
    #page.on('dialog', lambda dialog: asyncio.ensure_future(close_dialog(dialog)))
    #await page.screenshot({"path": "python.png"})
    html = await page.content()
    # close the browser
    await browser.close()
    return(html)
  #
  #url="https://careers.microsoft.com/professionals/us/en/search-results?from=80&s=1&rk=l-c-hardware-engineering"
  html=asyncio.get_event_loop().run_until_complete(main(url=url))
  
  return html


from datetime import date
from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request

def extract_links_from_html(url: str, html: str) -> pd.DataFrame:
  """
    Parses HTML for links and returns them as directed dyads in a pandas dataframe

    :param url: The url of the page hosting the links.
    :type arg2: string
    :param html: The HTML of a webpage.
    :type arg1: string
    :returns: Directed dyads of links on a website
    :rtype: pandas.DataFrame

  """
  soup = BeautifulSoup(html, "html.parser")
  link_list=[]
  for link in soup.findAll('a'): 
    try:
      link_list.append({'text_b':link.text.strip(), 'url_b':link['href'].strip()})
    except:
      #print("No Text")
      link_list.append({'text_b':link.text.strip(), 'url_b': None})
  #  
  link_dyads= pd.DataFrame(link_list)
  link_dyads['url_a']=url.strip()
  link_dyads['html_title_a']=soup.find_all('title')[0].get_text().strip()
  link_dyads['date_observed']=date.today().isoformat()

  return link_dyads

from urllib.parse import urlparse
import pandas as pd
import duckdb

def push_links_to_db(link_dyads: pd.DataFrame, database) -> bool:
  """Takes a directed link dyad dataframe and pushes it to the database of your choice

    :param link_dyads: The directed link dyad dataframe
    :type arg1: pd.DataFrame
    :param database: The path to the database
    :type arg2: str
    :returns: Exception if raised
    :rtype: bool
  """
  try:
    with duckdb.connect(database, read_only=False) as con:
      con.query("""
        CREATE TABLE IF NOT EXISTS link_dyads (
            html_title_a text,
            url_a text,
            text_b text,
            url_b text,
            date_observed date);
        """)
        
      link_dyads.to_sql(
          name="link_dyads",
          con=con,
          if_exists="append",
          index=False
      )
  except Exception as err:
    print(f"Unexpected {err=}, {type(err)=}")
    raise

  return True

from urllib.parse import urlparse

def split_urls(urls: str) -> pd.DataFrame:
  urls_parsed=[urlparse(o) for o in set(urls)]
  urls_parsed_df = pd.DataFrame([{'url':o.geturl(),'url_netloc':o.netloc} for o in urls_parsed])
  urls_parsed_df['url_path'] = [o.path for o in urls_parsed]
  urls_parsed_df['url_params'] = [o.params for o in urls_parsed]
  urls_parsed_df['url_query'] = [o.query for o in urls_parsed]
  urls_parsed_df['url_fragment'] = [o.fragment for o in urls_parsed]
  return urls_parsed_df


    
def push_splitlinks_to_db(database: str) -> bool:
  
  with duckdb.connect(database, read_only=False) as con:
    con.execute("""
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_a_netloc text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_a_path text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_a_params text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_a_query text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_a_fragment text;
      
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_b_netloc text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_b_path text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_b_params text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_b_query text;
      ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_b_fragment text;
      """)
      
    urls_a=con.execute("""SELECT url_a 
                          FROM link_dyads 
                          WHERE url_a_netloc IS NULL;
          """).fetchdf().drop_duplicates()['url_a'].values.astype(str)
    if len(urls_a)>0:
      urls_a_split = split_urls(urls=urls_a)     
      urls_a_split.to_sql('temp_table', con, if_exists='replace')
      con.execute("""
          UPDATE link_dyads 
          SET url_a_netloc = temp_table.url_netloc,
           url_a_path = temp_table.url_path,
           url_a_params = temp_table.url_params,
           url_a_query = temp_table.url_query,
           url_a_fragment = temp_table.url_fragment
          FROM temp_table 
          WHERE link_dyads.url_a =temp_table.url;   
        """)     
      con.execute("""DROP TABLE temp_table;""")
            
    urls_b=con.execute("""SELECT url_b 
                          FROM link_dyads 
                          WHERE url_b_netloc IS NULL;
            """).fetchdf().drop_duplicates()['url_b'].values.astype(str)
    if len(urls_b)>0:   
      urls_b_split = split_urls(urls=urls_b)
      urls_b_split.to_sql('temp_table', con, if_exists='replace')
      con.execute("""
          UPDATE link_dyads 
          SET url_b_netloc = temp_table.url_netloc,
           url_b_path = temp_table.url_path,
           url_b_params = temp_table.url_params,
           url_b_query = temp_table.url_query,
           url_b_fragment = temp_table.url_fragment
          FROM temp_table 
          WHERE link_dyads.url_b =temp_table.url;""")
      con.execute("""DROP TABLE temp_table;""")

    #Add an absolute URL if it needs one
    con.execute("""ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS url_b_absolute text;""")
    con.execute("""UPDATE link_dyads 
                    SET url_b_absolute = url_b;""")
    
    con.execute("""UPDATE link_dyads 
                  SET url_b_absolute = concat_ws('', url_a_netloc, url_b) 
                  WHERE url_b_netloc IS NULL OR url_b_netloc='';   """)

    con.execute("""UPDATE link_dyads 
                    SET url_b_netloc = url_a_netloc
                    WHERE url_b_netloc IS NULL OR url_b_netloc==''
                    ;""")

    con.execute("""ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS netloc_same bool;""")
    con.execute("""UPDATE link_dyads SET netloc_same = (url_a_netloc=url_b_netloc);""")

    con.execute("""ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS netloc_same bool;""")
    con.execute("""UPDATE link_dyads SET netloc_same = (url_a_netloc=url_b_netloc);""")

  return True


def classify_links(database: str) -> bool:
  with duckdb.connect(database, read_only=False) as con:
    #con.execute("""ALTER TABLE link_dyads DROP COLUMN IF EXISTS text_b_class_mnli;""")
    con.execute("""ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS text_b_class_mnli text;""")
    text_b=con.execute("""SELECT text_b FROM link_dyads WHERE text_b_class_mnli IS NULL AND text_b <> '' ;""").fetchdf().drop_duplicates()['text_b'].values.astype(str)

    if len(text_b)>0:
      #print("Fitting")
      text_b_class_mnli = zero_shot_classifier(
                            texts=text_b,
                            model="facebook/bart-large-mnli",
                            candidate_labels = ['job title', 'company', 'career', 'industry', 
                                                'job detail', 'website navigation','symbols', 'webpage', 'url',
                                                'corporate directory','number', 'link on a job website', 'job website']
                          )
    
      text_b_class_mnli.to_sql('temp_table', con, if_exists='replace')
      con.execute("""
          UPDATE link_dyads 
          SET text_b_class_mnli = temp_table.text_class,
          FROM temp_table 
          WHERE link_dyads.text_b =temp_table.sequence;   
        """)     
      con.execute("""DROP TABLE temp_table;""")

  return True
  


def filter_links(database: str) -> bool:
  with duckdb.connect(database, read_only=False) as con:
    #con.execute("""ALTER TABLE link_dyads DROP COLUMN IF EXISTS keep;""")
    con.execute("""ALTER TABLE link_dyads ADD COLUMN IF NOT EXISTS keep bool;""")

    con.execute("""
        UPDATE link_dyads 
        SET keep = 
        --Our logic for which links to insert goes here
        --Link is legitimate
        (url_b_absolute NOT NULL AND url_b_absolute <> '' AND text_b NOT NULL AND text_b <> '' ) AND
        --Classified as a job title or very LONG 
        (text_b_class_mnli='job title' OR LENGTH(text_b)>80) 
        
        --and not classified as one of the clearly wrong categories
        AND NOT ( text_b_class_mnli IN ( 'company','webpage','job website','link on a job website','symbols','url','website navigation' )  );       
      """)
    
    con.execute("""
      CREATE TABLE IF NOT EXISTS job_posts (
          url text,
          url_domain text,
          company text,
          location text,
          date_posted date,
          date_scraped date,
          link_text text
          );
      """)
    
    con.execute("""
      INSERT INTO job_posts (url, url_domain, date_scraped, link_text)
          SELECT DISTINCT 
          url_b_absolute as url,
          url_b_netloc as url_domain,
          date_observed as date_scraped,
          text_b as link_text 
          FROM link_dyads as ld
  
          WHERE
          --Link is legitimate
          (ld.url_b_absolute NOT NULL AND ld.url_b_absolute <> '' AND ld.text_b NOT NULL AND ld.text_b <> '' )  AND
          --Not already in
          NOT EXISTS(SELECT url FROM job_posts as jp WHERE ld.url_b_absolute = jp.url) AND
          ld.keep = TRUE
      """)
    
  return True



from datetime import date
from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request

def extract_text_from_html(url: str, html: str) -> pd.DataFrame:
  """
    Parses HTML for visible text pandas dataframe

    :param url: The url of the page hosting the links.
    :type arg2: string
    :param html: The HTML of a webpage.
    :type arg1: string
    :returns: Visible text of a website
    :rtype: pandas.DataFrame

  """
  soup = BeautifulSoup(html, "html.parser")
  #
  #Get visible text
  #https://stackoverflow.com/questions/1936466/how-to-scrape-only-visible-webpage-text-with-beautifulsoup

  def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
      return False
    if isinstance(element, Comment):
        return False
    return True
  #
  def text_from_html(soup):
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)
  #  
  #Extract all the features we want
  html_title=soup.find_all('title')[0].get_text()
  html_text=text_from_html(soup)  
  #
  df= pd.DataFrame({"url": [url]} )
  df['html_title']=soup.find_all('title')[0].get_text().strip()
  df['date_observed']=date.today().isoformat()
  df['html_text']=html_text
  
  return df


def push_text_to_db(job_post: pd.DataFrame, database) -> bool:
  """Takes a directed link dyad dataframe and pushes it to the database of your choice

    :param link_dyads: The directed link dyad dataframe
    :type arg1: pd.DataFrame
    :param database: The path to the database
    :type arg2: str
    :returns: Exception if raised
    :rtype: bool
  """

  with duckdb.connect(database, read_only=False) as con:
      con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_title text;""")
      con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS date_observed date;""")
      con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text text;""")
      job_post.to_sql('temp_table', con, if_exists='replace')
      con.execute("""
          UPDATE job_posts 
          SET html_title = temp_table.html_title,
              date_observed = temp_table.date_observed,
              html_text = temp_table.html_text,
          FROM temp_table 
          WHERE job_posts.url =temp_table.url;   
        """)     
      con.execute("""DROP TABLE temp_table;""")
  return True

    
    
    

#Note currently implimented in duckdb
#NotImplementedException: Not implemented Error: ColumnDef type not handled yet
def drop_db_duplicates(database: str) -> bool:
  """Takes a path to the databaes and runs depulication on the two main tables
    :param database: The path to the database
    :type arg1: str
    :returns: Exception if raised
    :rtype: bool
  """
  
  con.query("""CREATE TABLE link_dyads_temp (LIKE link_dyads);""")
  
  try:
    with duckdb.connect(database, read_only=False) as con:
      con.query("""
      CREATE TABLE link_dyads_temp (LIKE link_dyads);
      
      -- step 2
      INSERT INTO link_dyads_temp
      SELECT 
          DISTINCT *
      FROM link_dyads; 
      
      -- step 3
      DROP TABLE link_dyads;
      
      -- step 4
      ALTER TABLE link_dyads_temp 
      RENAME TO link_dyads; 
    """
    )
  except Exception as err:
    print(f"Unexpected {err=}, {type(err)=}")
    raise

  return True
