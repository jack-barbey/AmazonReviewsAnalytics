## Files ##

### 1000_final.csv ###

Contains cleaned results of MRJob on a subset of the "Automotive" subset, using the first 1000 reviews only.

### all_words_dict.pkl ###

A dictionary mapping all words contained in the "Automotive" subset of reviews to unique indices in a list. Used as part of calculating the cosine distance of a pair of reviews in final_job_auto.py.

### cleaner.py ###

Changes the output of final_job_auto.py from a tab-delineated .txt file into a properly formatted .csv file. Example: `[1, 0.01, 1.0],   248` becomes `1, 0.01, 1.0, 248`.

### final_job_auto.py ###

Contains the primary MRJob, which analyzes all possible pairs of review data and produces a .txt file. To increase efficiency, there are multiple yields within this job, mostly focused on exploring the connection between review similarity (measured by cosine distance) and attributes of the review and the product.

### find_all_words_mr.py ###

Contains an MRJob that finds all words that appear in at least one review. The output is found in words.txt.

### rebin.py ###

Contains an MRJob that transforms the output of final_job_auto.py. Specifically, if a ratio of two prices was originally `larger_price / smaller_price * 100%`, it corrects this to `smaller_price / larger_price * 100%`. This is necessary because our original format in final_job_auto.py made it difficult to meaningfully graph our results, and we did not want to waste resources redoing excessive amounts of calculation. For demonstration purposes, the final_job code therefore remains "wrong," maintaining the necessity of rebin.py.

### words.txt ###

Contains all unique words in the "Automotive" subset. Used to create all_words_dict.pkl.

## Results (example) ##

In the automotive subset, it is not clear that products with very similar prices have more similar reviews than average.

![Automotive Image](./automotive_image.png?raw=true "Prof. Wachs is thebomb.com")
