# Copyright Rex W, Douglass 2023
# SPDX-License-Identifier: Apache-2.0

"""
This is an NLP pipeline that performs zero shot classification of short texts into categories and returns a pandas dataframe with answers.
"""

import pandas as pd
import re
from transformers import AutoModelForCausalLM, AutoTokenizer
import torch
from transformers import pipeline
from torch.utils.data import Dataset
from tqdm.auto import tqdm
from datasets import concatenate_datasets, load_dataset
from datasets import Dataset

def zero_shot_classifier(texts: str,
                         candidate_labels = ['job title', 'company', 'career', 'industry', 
                         'job detail', 'website navigation','symbols', 'webpage', 'url', 'corporate directory','number', 'link on a job website', 'job website'],
                         model: str ="facebook/bart-large-mnli",
                         device: str ="cuda:0",
                         verbose: bool =True
                         ) -> pd.DataFrame:
  """
    Takes in a vector of texts applies and applied a zero shot classification model. Returns a dataframe with URL-Answer pairs and scores.
    :param texts: Vector of texts to be classified.
    :param candidate_labels: The categories for the model to apply nearest neighbor semantic classiciation toward.
    :param model: Huggingface model to apply
    :param device: cuda or cpu device to execute model on
    :param verbose: Whether to print diagnostic information prior to run
    :type arg1: str
    :type arg2: str
    :type arg3: str
    :type arg4: str
    :type arg5: bool
    :returns: Pandas data frame with String-Category/Score rows
    :rtype: pd.DataFrame
  """
  if verbose:
    torch.cuda.is_available()
    torch.cuda.device_count()
    torch.cuda.current_device()
    torch.cuda.device(0)
    torch.cuda.get_device_name(0)
    
  device = torch.device("cuda:0")

  classifier = pipeline("zero-shot-classification",
                        model="facebook/bart-large-mnli", device="cuda:0")

  #sequence_to_classify = "one day I will see the world"
  #candidate_labels = ['job title', 'company name', 'job detail', 'website navigation','symbols', 'webpage', 'url', 'corporate directory','number', 'link on a job website', 'job website']
  #classifier(sequence_to_classify, candidate_labels)
  classified = pd.DataFrame( [classifier(text, candidate_labels) for text in set(texts) if text!=''] ) #it doesn't like empties
  classified['text_class'] = [ q[0] for q in classified['labels'] ]

  return classified


