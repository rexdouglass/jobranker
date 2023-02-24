# Copyright Rex W, Douglass 2023
# SPDX-License-Identifier: Apache-2.0

"""
These utilities govern feature extraction from job posts
"""

import pandas as pd
import duckdb
from jobranker.common.question_answering import *

def qa_company(database: str, question: str ="What company is this job at?") -> bool:
  #QA Company from Post Text
  with duckdb.connect(database=database) as con:
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_company_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_company_electra_score double;""") #
    texts =pd.read_sql("SELECT html_text FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_company_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls =pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_company_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_text_company_electra = temp_table.answer,
                        html_text_company_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url =temp_table.url;""")
      con.execute("""DROP TABLE temp_table;""")

  #QA Company from Page Title
  with duckdb.connect(database=database) as con:
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_title_company_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_title_company_electra_score double;""") #
    texts =pd.read_sql("SELECT html_title FROM job_posts WHERE html_title IS NOT NULL AND LENGTH(html_title)>10  AND html_title_company_electra IS NULL;", con).values.astype(str)
    urls = pd.read_sql("SELECT url        FROM job_posts WHERE html_title IS NOT NULL AND LENGTH(html_title)>10  AND html_title_company_electra IS NULL;", con).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_title_company_electra       = temp_table.answer,
                        html_title_company_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url = temp_table.url;""")
      con.execute("""DROP TABLE temp_table;""")
  return True 



def qa_location(database: str, question: str = "What city is this job at?") -> bool:
  #QA location from Post Text
  with duckdb.connect(database=database, read_only=False) as con: 
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_location_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_location_electra_score double;""") #
    
    texts =pd.read_sql("SELECT html_text FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_location_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls =pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_location_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)

    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_text_location_electra = temp_table.answer,
                        html_text_location_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url =temp_table.url;""")     
      con.execute("""DROP TABLE temp_table;""")
  
  #QA location from Page Title
  with duckdb.connect(database=database, read_only=False) as con: 
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_title_location_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_title_location_electra_score double;""") #
    
    texts =pd.read_sql("SELECT html_title FROM job_posts WHERE html_title IS NOT NULL AND LENGTH(html_title)>10  AND html_title_location_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls = pd.read_sql("SELECT url        FROM job_posts WHERE html_title IS NOT NULL AND LENGTH(html_title)>10  AND html_title_location_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_title_location_electra       = temp_table.answer,
                        html_title_location_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url = temp_table.url;""")     
      con.execute("""DROP TABLE temp_table;""")

  return True

    

def qa_job_title(database: str, question: str = "What is the job title of this position?") -> bool:
  
  #QA job_title from Post Text
  with duckdb.connect(database=database, read_only=False) as con: 
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_job_title_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_job_title_electra_score double;""") #
    
    texts =pd.read_sql("SELECT html_text FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_job_title_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls =pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_job_title_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)

    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_text_job_title_electra = temp_table.answer,
                        html_text_job_title_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url =temp_table.url;""")     
      con.execute("""DROP TABLE temp_table;""")
  
  #QA job_title from Page Title
  with duckdb.connect(database=database, read_only=False) as con: 
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_title_job_title_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_title_job_title_electra_score double;""") #
    
    texts =pd.read_sql("SELECT html_title FROM job_posts WHERE html_title IS NOT NULL AND LENGTH(html_title)>10  AND html_title_job_title_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls = pd.read_sql("SELECT url        FROM job_posts WHERE html_title IS NOT NULL AND LENGTH(html_title)>10        AND html_title_job_title_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_title_job_title_electra       = temp_table.answer,
                        html_title_job_title_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url = temp_table.url;""")     
      con.execute("""DROP TABLE temp_table;""")

  #QA job_title from Link
  with duckdb.connect(database=database, read_only=False) as con: 
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS link_text_job_title_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS link_text_job_title_electra_score double;""") #
    
    texts =pd.read_sql("SELECT link_text  FROM job_posts WHERE link_text IS NOT NULL AND LENGTH(html_title)>5  AND   link_text_job_title_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls = pd.read_sql("SELECT url        FROM job_posts WHERE link_text IS NOT NULL AND LENGTH(html_title)>5        AND link_text_job_title_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET link_text_job_title_electra       = temp_table.answer,
                        link_text_job_title_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url = temp_table.url;""")     
      con.execute("""DROP TABLE temp_table;""")  

  #Update job title best using our new categories
  #job_title_best
  with duckdb.connect(database=database, read_only=False) as con: 
    con.execute("""ALTER TABLE job_posts DROP COLUMN IF EXISTS job_title_best;""")
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS job_title_best text;""")
    con.execute("""UPDATE job_posts SET job_title_best = TRIM(link_text_job_title_electra) WHERE (job_title_best IS NULL OR job_title_best='') ; """)
    con.execute("""UPDATE job_posts SET job_title_best = TRIM(html_title_job_title_electra) WHERE (job_title_best IS NULL OR job_title_best='') ; """)
    con.execute("""UPDATE job_posts SET job_title_best = TRIM(html_text_job_title_electra) WHERE (job_title_best IS NULL OR job_title_best='') ; """)

  return True



def qa_salary(database: str, question: str ="What is the dollar range of this job?") -> bool:
  #QA salary from Post Text
  with duckdb.connect(database=database) as con:
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_salary_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_salary_electra_score double;""") #
    texts =pd.read_sql("SELECT html_text FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_salary_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls =pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_salary_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_text_salary_electra = temp_table.answer,
                        html_text_salary_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url =temp_table.url;""")
      con.execute("""DROP TABLE temp_table;""")
      
  return True 


def qa_qualifications(database: str, question: str = "What are the qualifications of this job?") -> bool:
  #QA qualifications from Post Text
  with duckdb.connect(database=database) as con:
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_qualifications_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_qualifications_electra_score double;""") #
    texts =pd.read_sql("SELECT html_text FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_qualifications_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls =pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_qualifications_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_text_qualifications_electra = temp_table.answer,
                        html_text_qualifications_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url =temp_table.url;""")
      con.execute("""DROP TABLE temp_table;""")
  return True 


def qa_education(database: str, question: str = "What education is required for this job?") -> bool:
  #QA education from Post Text
  with duckdb.connect(database=database) as con:
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_education_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_education_electra_score double;""") #
    texts =pd.read_sql("SELECT html_text FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_education_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls =pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_education_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_text_education_electra = temp_table.answer,
                        html_text_education_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url =temp_table.url;""")
      con.execute("""DROP TABLE temp_table;""")
  return True 


def qa_languages(database: str, question: str = "What programming languages are required for this job?") -> bool:
  #QA languages from Post Text
  with duckdb.connect(database=database) as con:
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_languages_electra text;""") #
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS html_text_languages_electra_score double;""") #
    texts =pd.read_sql("SELECT html_text FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_languages_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    urls =pd.read_sql("SELECT url FROM job_posts WHERE html_text IS NOT NULL AND LENGTH(html_text)>50  AND html_text_languages_electra IS NULL;", con, parse_dates=["date_observed"]).values.astype(str)
    if len(texts)>0:
      answers=question_answer( texts=texts , urls=urls, question = question)
      answers.to_sql('temp_table', con, if_exists='replace')
      con.execute("""UPDATE job_posts 
                    SET html_text_languages_electra = temp_table.answer,
                        html_text_languages_electra_score = temp_table.score,
                    FROM temp_table 
                    WHERE job_posts.url =temp_table.url;""")
      con.execute("""DROP TABLE temp_table;""")
  return True 

def regex_remote(database: str) -> bool:
  #Regex terms for remote
  with duckdb.connect(database=database) as con:
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS remote integer;""") #
    con.execute("""UPDATE job_posts SET remote = 0 ; """)
    con.execute("""UPDATE job_posts SET remote = remote + 1 WHERE LOWER(html_text) LIKE '%%remote%%' ; """)
    con.execute("""UPDATE job_posts SET remote = remote + 1 WHERE LOWER(html_title) LIKE '%%remote%%' ; """)
    con.execute("""UPDATE job_posts SET remote = remote + 1 WHERE LOWER(link_text) LIKE '%%remote%%'  ; """)
    con.execute("""UPDATE job_posts SET remote = remote + 1 WHERE LOWER(html_text_location_electra) LIKE '%%remote%%'  ; """)
  return True 




  
