import sys

'''
Usage: python3 cleaner.py <to_be_cleaned.csv> <cleaned.csv>
'''

def clean(infile, outfile):
    '''
    Removes brackets and spaces from the csv output.
   
    '''

    g = open(outfile, "w")
    g.write("category,x_var,y_var,weight\n")

    with open(infile, "r") as f:
        for line in f:
            new = line.replace("[", "")
            new = new.replace("]\t", ", ")
            g.write(new)

    g.close()

if __name__ == '__main__':
    clean(sys.argv[1], sys.argv[2])
