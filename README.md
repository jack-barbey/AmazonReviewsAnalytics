# Amazon Reviews Analytics #

A CS123 project by Jack Barbey, Fiona Gasaway, and Peter Wiggin

## The Data ##

We worked with Amazon product and review data, which contains information such as the rating of the product, the price, the review text, and the helpfulness of the reviews. The data ([found here](http://jmcauley.ucsd.edu/data/amazon/links.html)) was provided by Julian McAuley.

We used McAuley's 5-core dataset throughout his project. This is a subset of all Amazon reviews in which every product has at least 5 reviews and every reviewer wrote at least 5 reviews. The full 5-core dataset is 9.9 GB and contains 41 million reviews. The full metadata (product information) is 3.1 GB and contains 9.4 million unique products.

The largest review dataset that we analyzed is the 5-core subset "Electronics," which has 2 million reviews. For the script computing cosine similarity, the largest subset we used was 2,000 reviews, forming 4,000,000 possible pairs. This took 4.5 hours on 21 cores on the Google Cloud Platform. 

## Directories ##

### automotive ###

Analyzes pairs of reviews in the "Automotive" subset of the review data using MRJob, including calculating the cosine similarity of reviews.

### cell_phones ###

Analyzes pairs of reviews in the "Cell Phones and Accessories" subset of the review data using MRJob, including calculating the cosine similarity of reviews.

### electronics ###

Analyzes individual reviews in the "Electronics" subset of the review data using MRJob. Analyzing individual reviews is much less expensive than analyzing pairs of reviews, so this process was applied to the largest subset we used.

### instruments ###

Analyzes pairs of reviews in the "Musical Instruments" subset of the review data in MRJob, without calculating the cosine similarity of reviews. Since calculating cosine similarity is necessarily expensive, dropping this variable enables iteration over more pairs in a given amount of time.

### graphing ###

Visualizes the results of our MRJob processes using D3.

### helper_functions ###

A collection of files that are useful in many or all of the MRJobs.
