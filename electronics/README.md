## Files ##

### cleaned1.csv, cleaned2.csv ###

Final results from job_not_pairs.py

### cleaner_special.py ###

Simply removes brackets from a .csv file (the output from job_not_pairs.py) to make a fully cleaned .csv results.

### job_not_pairs.py ###

Contains the primary MRJob, which analyzes all reviews (not pairs!) and should be piped to a .csv file. Looking at individual reviews is much faster than looking at pairs, so this can be run on a much larger dataset than the primary MRJobs in other directories.
