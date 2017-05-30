import mrjob.job
import sys

'''
Usage: python3 rebin.py <old_data.csv> > <rebined_data.csv>
'''

class reverse(mrjob.job.MRJob):
    '''
    Takes in a csv file containing output from a job that contains percent ratios greater than 100%
    and inverts the percents. For example, 125% becomes 80%.
    Produces a new CSV file.
    This enhances binning by keeping all percents between 0 and 100, instead of 0 and infinity.
    '''
    
    # This protocol lets sets be used
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def mapper(self, _, line):
        line_list = line.split(",")
        nums = []
        for element in line_list:
            new = float(element.strip())
            nums.append( new )

        nums[0] = int(nums[0])
        nums[3] = int(nums[3])

        # Inverts ratios 
        if nums[0] == 5:
            new = int( 100/ (nums[1] / 100))
            yield [5, new, nums[2]], nums[3]
        elif nums[0] == 6:
            new = int( 100/ (nums[2] / 100) )
            yield [6, nums[1], new], nums[3]
        elif nums[0] == 4:
            week = int(nums[2]/ 7)
            yield [4, nums[1], week], nums[3]
        else:
            yield [nums[0], nums[1], nums[2]], nums[3]





    def combiner(self, word, counts):
        yield word, sum(counts)



    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == '__main__':
    reverse.run()
