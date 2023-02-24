# Copyright Rex W, Douglass 2023
# SPDX-License-Identifier: Apache-2.0

"""
JobRanker main application
Run nightly with a chron job
"""

#Relative Path to DuckDB database
database='jobranker/database/jobranker.duckdb'

#Imports
import numpy as np
import pandas as pd
import duckdb
import pygsheets

from jobranker.common.helpers import *

from jobranker.common.zero_shot_classifier import *
from jobranker.common.link_classifier import *

from jobranker.common.question_answering import *
from jobranker.common.post_feature_extraction import *

from jobranker.common.ranker import *

with duckdb.connect(database=database, read_only=False) as con:
  link_dyads=pd.read_sql("SELECT * FROM link_dyads;", con, parse_dates=["date_observed"])
  LinkLabels=pd.read_csv("/mnt/8tb_a/rwd_github_private/DataJobsByRexDouglass/jobranker/database//LinkLabels.csv")

#Download and process the seed URLs from live googlesheet
#https://docs.google.com/spreadsheets/d/1G8omZkflKz6xa7hb2pqvjbWarnSMXbLUInM1XITF2RE/edit?usp=sharing
sheet_id="1G8omZkflKz6xa7hb2pqvjbWarnSMXbLUInM1XITF2RE"
sheet_name="linkstojobposts"
url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={sheet_name}"
seed_urls=load_google_sheet(sheet_id=sheet_id,sheet_name=sheet_name)['url'].values
len(seed_urls)
seed_urls_remaining = set(seed_urls).difference(set(link_dyads['url_a'].values))
len(seed_urls_remaining)

#Iterative over and scrape seed URLS
for url in seed_urls_remaining:
  print(url)
  html=scrape_url(url)
  link_dyads=extract_links_from_html(url=url, html=html)
  push_links_to_db(link_dyads=link_dyads, database=database)

#https://duckdb.org/docs/archive/0.3.2/api/python.html
push_splitlinks_to_db(database=database)

classify_links(database=database)

filter_links(database=database)

#Scrape Job Posts and Store Them
with duckdb.connect(database=database, read_only=False) as con:
  con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text text;""")
  post_urls_remaining = pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NULL;", con)['url'].values.astype(str)
  print(len(post_urls_remaining))
  for url in post_urls_remaining:
    print(url)
    try:
      html=scrape_url(url)
      job_post=extract_text_from_html(url=url, html=html)
      push_text_to_db(job_post=job_post, database=database)
    except:
      print("Failed on " + url)
      
#Question Answering from Job Posts Themselves
qa_company(       database=database, question  = "What company is this job at?"  )
qa_location(      database=database, question  = "What city is this job at?" )
qa_job_title(     database=database, question  = "What is the job title of this position?" )
qa_salary(        database=database,  question  ="What is the dollar range of this job?" )
qa_qualifications(database=database,  question  = "What are the qualifications of this job?" )
qa_education(     database=database,  question  = "What education is required for this job?" ) 
qa_languages(     database=database, question  = "What programming languages are required for this job?" )
regex_remote(database=database)

#Weak Supervision Ranking
ranker(database=database)

#Pull ranked jobs and push them back to google sheets for viewing
with duckdb.connect(database=database, read_only=False) as con: 
  job_posts=pd.read_sql("SELECT rex_rank, job_title_best, remote, url  FROM job_posts ORDER BY rex_rank DESC;", con, parse_dates=["date_observed","date_posted"])

  gc = pygsheets.authorize(service_file="/home/skynet3/Downloads/client_secret.json") #Local secret file, will need your own
  sh=gc.open_by_key('1S7nqIVHYzNPh0RlnNMBBlKh5l40E6P67_KdUtpOJK50')
  worksheet = sh.worksheet('title','CandidatePosts')
  worksheet.set_dataframe(job_posts.head(1000),(1,1))


