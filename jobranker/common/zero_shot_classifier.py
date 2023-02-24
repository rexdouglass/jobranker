#Zero shot classification of link titles
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


