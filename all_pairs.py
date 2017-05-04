import math
import numpy as np
import pickle


def load_obj(name):
    with open(name, 'rb') as f:
        obj = pickle.load(f)
        num_words = obj[0]
        all_words_dict = obj[1]
        return num_words, all_words_dict



'''
def ln_vector_cos(v1, v2):
     ***NOT DONE*** Computes the ln of the cosine distance
   prod = np.dot(v1, v2)
   len1 = math.sqrt(np.dot(v1, v1))
   len2 = math.sqrt(np.dot(v2, v2))
   return prod / (len1 * len2)
'''

if __name__ == '__main__':
    num_words, all_words_dict = load_obj("all_words_dict.pkl")
