# Copyright Rex W, Douglass 2023
# SPDX-License-Identifier: Apache-2.0

"""
This is where the logic for personalized ranking lies and for inserting those preferences
into the database
"""

def adjust_rank(score: int, criteria: str) -> bool:
  """
    Takes in a score to adjust the rank by and an SQL formatted where statement to append and execute.
    :param score: The desired adjusment to score, positive or negative
    :param criteria: The name of the specific sheet taken from the tab within the google sheet
    :type arg1: int
    :type arg2: str
    :returns: Success
    :rtype: bool
  """
    with duckdb.connect(database=database, read_only=False) as con: 
      con.execute("""UPDATE job_posts SET rex_rank = rex_rank + """ + str(score) + """ WHERE """ + criteria )
    return True

def ranker(database):
  """
    Applies personalized logic to rank job posts
    :param database: The DUckDB database location
    :type arg1: str
    :returns: Success
    :rtype: bool
  """
  with duckdb.connect(database=database, read_only=False) as con: 
    con.execute("""ALTER TABLE job_posts DROP COLUMN IF EXISTS rex_rank;""")
    con.execute("""ALTER TABLE job_posts ADD COLUMN IF NOT EXISTS rex_rank integer;""")
    con.execute("""UPDATE job_posts SET rex_rank = 0""")

    adjust_rank(1, """LOWER(job_title_best) LIKE '%data scientist%' """ )
    adjust_rank(1, """LOWER(job_title_best) LIKE '%machine learning%' """ )
    adjust_rank(1, """LOWER(job_title_best) LIKE '%applied scientist%' """ )
    adjust_rank(1, """LOWER(job_title_best) LIKE '%data engineer%' """ )
    adjust_rank(1, """LOWER(job_title_best) LIKE '%data analyst%' """ )

    adjust_rank(-10, """LOWER(job_title_best) LIKE '%sales%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%data entry%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%medicine%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%law%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%service%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%manual%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%factory%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%machinist%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%software engineer%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%web developer%' """ )


    #seniority  
    adjust_rank(1, """LOWER(job_title_best) LIKE '%director%' """ )
    adjust_rank(1, """LOWER(job_title_best) LIKE '%principal%' """ )
    adjust_rank(1, """LOWER(job_title_best) LIKE '%senior%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%internship%' """ )
    adjust_rank(-10, """LOWER(job_title_best) LIKE '%entry%' """ )

    #location
    #+1 for sd in careeronestop
    adjust_rank(1, """ html_text_location_electra LIKE '%San Diego%' """ )


    #Additional Criteria Ommitted for Presentation
  
