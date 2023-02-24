# Copyright Rex W, Douglass 2023
# SPDX-License-Identifier: Apache-2.0

"""
This is an NLP pipeline that performs question answering over short texts and returns a pandas dataframe with answers.
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
from transformers import AutoModelForQuestionAnswering, AutoTokenizer, pipeline

def question_answer( texts: str,
                     urls: str,
                     question = "What company is this job at?",
                     model: str ="deepset/electra-base-squad2",
                     device: str ="cuda:0",
                     verbose: bool =True
                     ) -> pd.DataFrame:
  """
    Takes in a vector of texts and urls. Applies a question to a given QA model from huggingface. Returns a dataframe with URL-Answer pairs and scores.
    :param texts: Vector of texts to be extracted from (refered to as 'contexts' by the model)
    :param urls: URLs for mapping back to job post database
    :param question: Question to pose
    :param model: Huggingface model to apply
    :param device: cuda or cpu device to execute model on
    :param verbose: Whether to print diagnostic information prior to run
    :type arg1: str
    :type arg2: str
    :type arg3: str
    :type arg4: str
    :type arg5: str
    :type arg6: bool
    :returns: Pandas data frame with URL-Answer/Score rows
    :rtype: pd.DataFrame
  """
  if verbose:
    torch.cuda.is_available()
    torch.cuda.device_count()
    torch.cuda.current_device()
    torch.cuda.device(0)
    torch.cuda.get_device_name(0)
    
  device = torch.device("cuda:0")

  nlp = pipeline('question-answering',
                  model=model,
                  tokenizer=model,
                  device="cuda:0")
                  
  #Greater success without newlines
  texts=[q[0].replace("\n", " ").strip() for q in texts.tolist()]
  
  QA_input = [{
      'question': question,
      'context': text.strip()
  } for text in texts]
  answers = pd.DataFrame( nlp(QA_input, top_k=1, max_answer_len=20, handle_impossible_answer=True) ) 
  answers['url'] = urls
  
  return answers


