## Files ##

### all_words_dict.py ###

A template file to collect all unique words in a file of reviews and create a Pickle file vocab.pkl to store them for use in calculating cosine distances. Creates a dictionary where each word from the output of find_all_words_mr.py is mapped to an integer to be used later as a vector index.

### cleaner.py ###

A template file to change the output of a primary MRJob from a tab-delineated .txt file into a properly formatted .csv file. Example: `[0, 1, -1],   50` becomes `0, 1, -1, 50`. This may be customized within each directory given possible formatting differences.

### compress_json.py ###

Compress a .json file into a .json.gz file. To get a subset of a file called reviews.json.gz with only N reviews:
1) Open the file to decompress it (now reviews.json)
2) Take the head of the file: `>> head -N reviews.json > subset_reviews.json`
3) Use compress_json.py to compress it: `>> python3 compress_json.py subset_reviews.json subset_reviews.json.gz`

### csv_output_concept.py ###

This was used for practice with creating a .csv file (instead of a .txt file) with MRJob

### find_all_words_mr.py ###

Contains an MRJob that finds all words that appear in at least one review, along with the count of each word.

### job_with_cossim.py ###

A template for a primary MRJob, analyzing all possible pairs of review data and piping to a .txt file. To increase efficiency, there are multiple yields within this job. This file is customized in each folder to yield the exact variables desired and avoid unnecessary computation.

### make_metadata_db.py ###

Makes a SQLite database called metadata_db to store product information from a metadata file. This can be used to find the price of a product being reviewed, among other factors. Currently, it only stores the most necessary information (the "products" table) so as to save space, but by un-commenting 4 lines, you can produce a database that captures 100% of the information contained in the metadata files.

### make_reviews_db.py ###

Makes a SQLite database to store all information from a file of reviews. This was not ultimately used in our project (simply iterating over the reviews files suffices), but it might become useful during a later analysis of this data.

### rebin.py ###

A template for "re-binning" results from a primary MRJob that used too many small bins. We made this mistake several times, and rebin.py was very valuable, because it saved us from having to recompute everything from scratch. Specifically, this job converts percents to their inverses, e.g. 125% becomes 80%. Thus, our results can range from 0-100% instead of 100-infinity%.
