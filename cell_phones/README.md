## Files ##

### 2000_final.csv ###

Contains cleaned results of MRJob on a subset of the "Cell Phones and Accessories" subset, using the first 2000 reviews only.

### all_words_dict.py ###

Converts words_with_counts.txt into vocab.pkl.

### cleaner.py ###

Changes the output of job_cell_phones.py from a tab-delineated .txt file into a properly formatted .csv file. Example: `[1, 0.01, 1],   149` becomes `1, 0.01, 1, 149`.

### find_all_words_mr.py ###

Contains an MRJob that finds all words that appear in at least one review, as well as the number of times each word appears. The output is found in words_with_counts.txt.

### job_cell_phones.py ###

Contains the primary MRJob, which analyzes all possible pairs of review data and produces a .txt file. To increase efficiency, there are multiple yields within this job, mostly focused on exploring the connection between review similarity (measured by cosine distance) and attributes of the review and the product.

### rebin.py ###

Contains an MRJob that transforms the output of job_cell_phones.py. Specifically, if a ratio of two prices was originally `larger_price / smaller_price * 100%`, it corrects this to `smaller_price / larger_price * 100%`. This is necessary because our original format in job_cell_phones.py made it difficult to meaningfully graph our results, and we did not want to waste resources redoing excessive amounts of calculation. For demonstration purposes, the final_job code therefore remains "wrong," maintaining the necessity of rebin.py.

### results_2000.csv ###

An intermediate step in the attempt to create a cleaned .csv file (2000_final.csv) with our MRJob results for graphing purposes.

### vocab.pkl ###

A dictionary mapping all words contained in the "Cell Phones and Accessories" subset of reviews to unique indices in a list. Used as part of calculating the cosine distance of a pair of reviews in job_cell_phones.py.

### words_with_counts.txt ###

Contains all unique words in the "Automotive" subset, as well as the number of times each word appears. Used to vocab.pkl.
