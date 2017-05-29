import math
import numpy as np
import pickle
import sys
import io

'''
Usage: python3 all_words_dict.py <words_with_counts.txt> <vocab.pkl>
'''

def create_dictionary_all_words(infile, outfile):
    '''
    Using a file exported by mapreduce, this function maps each word used in every
    review to an ascending integer which will serve as an index for a spot in a list
    that becomes the vector. Two long lists will be all that are needed, as they can just
    be reset each time
    Then, it writes every pair to a file instead of keeping it in memory (✖╭╮✖)
    This file is a pickle file, of the tuple (num words, dictionary)
    '''
    all_words_dict = {}

    with open(infile, "r") as f:
        n = 0
        for line in f:
            word, count = line.split()
            count = int(count)
            if count > 1:
                word = line.split()[0].replace("\"", "")
                all_words_dict[word] = n
                n += 1

    # http://stackoverflow.com/questions/19201290/how-to-save-a-dictionary-to-a-file
    with open(outfile, 'wb' ) as f:
        pickle.dump((n+1, all_words_dict), f, pickle.HIGHEST_PROTOCOL)



    return all_words_dict, n+1
if __name__ == '__main__':
    create_dictionary_all_words(sys.argv[1], sys.argv[2])
