# Copyright Rex W, Douglass 2023
# SPDX-License-Identifier: Apache-2.0

"""
These utilities govern feature extraction from job posts
"""


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
