# This file converts Amazon metadata into a SQLite database.

# Usage: python3 make_metadata_db.py <category>
#            <.json.gz file with metadata>

import sqlite3
import json
import gzip
import sys

def parse(path):
    '''
    Parse json.gz file. Code taken from creator of our dataset:
    http://jmcauley.ucsd.edu/data/amazon/links.html
    '''
    g = gzip.open(path, 'r')
    for l in g:
        yield eval(l)

def create_products_table(c, category, data_gz_file):
    '''
    Create table with each product and their associated attributes.
    Only includes attributes that are single values.
    Join with other tables to find also_bought, categories, etc.
    '''
    c.execute("DROP TABLE IF EXISTS products_" + category)
    c.execute("CREATE TABLE products_" + category + ''' (
        productID TEXT PRIMARY KEY,
        title TEXT,
        price REAL,
        imUrl TEXT,
        brand TEXT,
        salesCategory TEXT,
        salesRank INT
        );''')

    insert_query = "INSERT INTO products_" + category + " VALUES (?,?,?,?,?,?,?)"
    products_columns = ["asin", "title", "price", "imUrl", "brand", "salesRank"]

    for product in parse(data_gz_file):
        vals = []
        for col_name in products_columns:
            if col_name == "salesRank":
                if col_name in product:
                    salesRankDict = product["salesRank"].items()
                    # sometimes salesRank tag exists, but dictionary is empty
                    if salesRankDict:
                        category, rank = list(product["salesRank"].items())[0]
                        vals.append(category)
                        vals.append(rank)
                    else:
                        vals.append(None)
                        vals.append(None)
                else:
                    vals.append(None)
                    vals.append(None)
            else:
                if col_name in product:
                    vals.append(product[col_name])
                else:
                    vals.append(None)
        c.execute(insert_query, tuple(vals))

def create_product_link_table(c, category, data_gz_file, link):
    '''
    Create table linking product to an associated product
    Link (str representing the relationship between products) can be:
        also_bought
        also_viewed
        bought_together
    '''
    table = link + "_"
    secondColumnID = "".join(x.capitalize() for x in link.split("_")) + "ID"
    secondColumnID = secondColumnID[0].lower() + secondColumnID[1:]
    c.execute("DROP TABLE IF EXISTS " + table + category)
    c.execute("CREATE TABLE " + table + category + ''' (
        productID TEXT NOT NULL,
        '''+secondColumnID+''' TEXT,
        PRIMARY KEY (productID, '''+secondColumnID+'''),
        FOREIGN KEY (productID) REFERENCES products_'''+category+''' (productID),
        FOREIGN KEY ('''+secondColumnID+''') REFERENCES products_'''+category+''' (productID)
        );''')

    insert_query = "INSERT INTO " + table + category + " VALUES (?,?)"

    for product in parse(data_gz_file):
        productID = product["asin"]
        if ("related" in product and link in product["related"]):
                match_list = product["related"][link]
                for match in match_list:
                    c.execute(insert_query, (productID, match))
        else:
            # Do we want productID, NULL pairs?
            # Or is just omitting the product better?
            # Easy to change later.
            c.execute(insert_query, (productID, None))

def create_categories_table(c, category, data_gz_file):
    '''
    Create table mapping products to their categories.
    Flattens categories, sub-categories, sub-sub-categories, etc. into one set
    '''
    c.execute("DROP TABLE IF EXISTS categories_" + category)
    c.execute("CREATE TABLE categories_" + category + ''' (
        productID TEXT NOT NULL,
        category TEXT,
        PRIMARY KEY (productID, category),
        FOREIGN KEY (productID) REFERENCES products_'''+category+''' (productID)
        );''')

    insert_query = "INSERT OR IGNORE INTO categories_" + category + " VALUES (?,?)"

    for product in parse(data_gz_file):
        productID = product["asin"]
        if "categories" in product:
            for cat_index in range(len(product["categories"])):
                for sub_cat in product["categories"][cat_index]:
                    c.execute(insert_query, (productID, sub_cat))
        else:
            # Do we want productID, NULL pairs?
            # Or is just omitting the product better?
            # Easy to change later.
            c.execute(insert_query, (productID, None))

def create_all_tables(category, data_gz_file):
    '''
    Create sqlite tables based on metadata in file
    '''
    db = sqlite3.connect("./metadata_db.sqlite")
    c = db.cursor()

    create_products_table(c, category, data_gz_file)
    '''
    create_product_link_table(c, category, data_gz_file, "also_bought")
    create_product_link_table(c, category, data_gz_file, "also_viewed")
    create_product_link_table(c, category, data_gz_file, "bought_together")
    create_categories_table(c, category, data_gz_file)
    '''

    c.close()
    db.commit()


if __name__ == "__main__":
    num_args = len(sys.argv)

    if num_args != 3:
        print("usage: python3 " + sys.argv[0] + " <category> " +
              "<.json.gz file with metadata>\n")
        sys.exit(0)

    create_all_tables(sys.argv[1], sys.argv[2])
