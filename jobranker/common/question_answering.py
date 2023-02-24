#Question Answering

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


