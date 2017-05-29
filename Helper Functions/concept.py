import mrjob.job
import re
from stop_words import get_stop_words

stop_words = set(get_stop_words('en'))

# Code inspired by http://stackoverflow.com/questions/6116978/python-replace-multiple-strings
# Removes punctuation to be replaced by spaces, good for fixing typos such as "the.end"
replace_chars = {".", "?", "!", "\\", "(", ")", ",", "/", "*", "&", "#",
                ";", ":", "-", "_", "=", "@", "[", "]", "+", "$", "~", "'", "`", '\\\"'}
replace_chars = set(re.escape(k) for k in replace_chars)
pattern = re.compile("|".join(replace_chars))



class find_all_words(mrjob.job.MRJob):
    # This protocol lets sets be used
    INTERNAL_PROTOCOL = mrjob.protocol.PickleProtocol

    def mapper(self, _, line):
        search = re.search("\"reviewText\": \"(.*)\", \"overall\"", line)
        chars_removed = pattern.sub(" ", search.group(1))
        words_list = chars_removed.lower().split()

        for word in words_list:
            if word in stop_words: continue
            yield word, None


    def combiner(self, word, _):
        yield word, None



    def reducer(self, word, _):
        yield word, None


if __name__ == '__main__':
    all_words = find_all_words.run()
