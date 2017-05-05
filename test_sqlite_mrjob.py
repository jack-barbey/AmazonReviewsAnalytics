# Evidence that we can't search SQLite table within MRJob

import mrjob.job
import re
import sqlite3


class SimpleJob(mrjob.job.MRJob):
    db = sqlite3.connect("./metadata_db.sqlite")
    c = db.cursor()

    # This protocol lets sets be used
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def mapper(self, _, line):
        productID = re.search("\"asin\": \"(.{10})\"", line).group(1)
        if productID:
            price = c.execute('''SELECT price FROM products_instruments AS p
                WHERE p.productID = ''' + productID)
            print(price)
            # yield productID, 1

    # def combiner(self, productID, counts):
    #     yield productID, sum(counts)

    # def reducer(self, productID, counts):
    #     yield productID, sum(counts)
    c.close()


if __name__ == '__main__':
    SimpleJob.run()