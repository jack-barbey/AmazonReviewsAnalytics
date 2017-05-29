from mrjob.job import MRJob 
import re



WORD_RE = re.compile(r"[\w]+")

class CSVOutputJob(MRJob):
    ''' Testing ways to output mrjob to CSV format

        python3 csv_output_concept.py <input file> > <output.csv>

        -you can yield any two objects to CSV directly and it will format well
        -for lists, the key value must be the second term and not in a list
        -however, if the key value is in fact a list, everything is fine 
        -everything in the list will be separated, but the [ ] will be present
            in the csv, this has something to do with yield versus return
        -the csv can then be fed through a cleaning script
    '''


    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            #yield word, 1
            obs = [1, len(word)]
            yield obs, 1

    def combiner(self, obs, counts):
        yield obs, sum(counts)

    def reducer(self, obs, counts):
        #yielding two objects makes them separate in CSV by default
        #yield str(word), sum(counts)

        #this does not work
        #yield (word, 1, "a"), sum(counts)

        #this does
        #yield (1, 2, 3), word

        #so does this, but the output has brackets printed in the file :/
        #num = sum(counts)
        #string = word
        #yield (1, "a", num), word

        #this also works 
        yield obs, sum(counts)


if __name__ == '__main__':
    CSVOutputJob.run()