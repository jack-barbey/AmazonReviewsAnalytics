## Files ##

### cleaner.py ###

Changes the output of job_cell_phones.py from a tab-delineated .txt file into a properly formatted .csv file. Example: `[0, 1, -1],   50` becomes `0, 1, -1, 50`.

### fast_results.csv ###

Contains cleaned results of MRJob on a subset of the "Cell Phones and Accessories" subset, using the first 2000 reviews only.

### full_instruments_out.csv ###

An intermediate step in the attempt to create a cleaned .csv file (fast_results.csv) with our MRJob results for graphing purposes.

### job_without_cossim.py ###

Contains the primary MRJob, which analyzes all possible pairs of review data and produces a .txt file. To increase efficiency, there are multiple yields within this job. Also, this job does not calculating the cosine similarity between reviews, restricting our choice of variable combinations but greatly increasing the speed and therefore the maximum feasible file size.
