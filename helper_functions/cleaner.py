

def clean(infile, outfile):
    '''
    Removes the brackets from the csv output.
    Change the file names in the last line to accordingly.

    python3 cleaner.py
    '''

    g = open(outfile, "w")

    with open(infile, "r") as f:
        for line in f:
            new = line.replace("[", "")
            new = new.replace("]", "")
            g.write(new)

    g.close()

if __name__ == '__main__':
    clean("output.csv", "cleanoutput.csv")