[![CC BY-NC-SA
4.0](https://img.shields.io/badge/License-CC%20BY--NC--SA%204.0-lightgrey.svg)](http://creativecommons.org/licenses/by-nc-sa/4.0/)

## JobRanker

This is a github repository for a personal project JobRanker which scrapes job postings, extracts key facts, and provides a personalized ranking for review.

## Authors:

[Rex W. Douglass](http://www.rexdouglass.com)

## The Data:

The repository includes a demostration DuckDB database as well as publically facing google sheets for the inpust seed urls and output job rankings. The reliability of either is subject to change and the user is encouraged to generate their own examples from scratch on a fresh run.

## Replication Code and Analysis

The repository is organized around a module "jobranker" in the root folder. Within you will find the main point of entry "app.py" which can be run nightly using a scheduler such as chron. The main functions are contained in the "common" module and are organized as follows:

1. Web Crawler
 * webscraper.py - Responsible for collecting and parsing HTML
 * zero_shot_classifier.py - NLP pipeline for zero shot classification of text into categories
 * link_classifier.py - Logic for classifying links as "To a job posting" or not.
2. Post Extractor
 * question_answering.py - NLP pipeline for question answering from short spans of text.
 * post_feature_extraction.py - Applies question answering to job posts to extrcat and clean key facts
3. Poster Ranker
 * ranker.py - Produces a ranking over job posts using weak supervision and a list of hand written rules

### License

This work is licensed under a [Creative Commons
Attribution-NonCommercial-ShareAlike 4.0 International
License](http://creativecommons.org/licenses/by-nc-sa/4.0/).

[![CC BY-NC-SA
4.0](https://licensebuttons.net/l/by-nc-sa/4.0/88x31.png)](http://creativecommons.org/licenses/by-nc-sa/4.0/)
