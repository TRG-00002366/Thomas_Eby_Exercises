from pyspark import SparkContext
import re

sc = SparkContext("local[*]", "Sentence Analysis")

whole_text_rdd = sc.wholeTextFiles("1-PySpark/mini_project/data")

chapters = set(['A Scandal in Bohemia', 'The Red-headed League', 'A Case of Identity', 'The Boscombe Valley Mystery', 'The Five Orange Pips', 'The Man with the Twisted Lip',
    'The Adventure of the Blue Carbuncle', 'The Adventure of the Speckled Band', "The Adventure of the Engineer's Thumb", 'The Adventure of the Noble Bachelor', 'The Adventure of the Beryl Coronet',
    'The Adventure of the Copper Beeches'])
chapter_pattern = "(" + "|".join(re.escape(name.lower()) for name in chapters) + ')'
bc_chapters = sc.broadcast(chapter_pattern)

# First split around roman numerals since they interfere with splitting around the period
# Then split around period excluding formal abbreviations like Mr., Mrs., Dr.
# Then split around question mark and exclamation points
# Then replace line breaks with a space and strip space on either end
# Then split around the chapter titles
# Finally remove quotation marks
# Sentences remain
sentences = whole_text_rdd.flatMap(lambda pair: re.split(r'\r\n\s*(?:III|II|I|IV|V|VIII|VII|VI|IX|X|XII|XI)\.', pair[1])) \
    .flatMap(lambda line: re.split(r'(?<![A-Z]r)(?<![A-Z]rs)\.', line)) \
    .flatMap(lambda line: re.split(r'[?!]', line)) \
    .map(lambda sentence: sentence.replace('\r\n', ' ').strip()) \
    .flatMap(lambda sentence: sentence.lower().split(chapter_pattern)) \
    .map(lambda sentence: sentence.replace('"', ' ').strip()) \
    
#print(sentences.take(20))

longest_line = sentences.map(lambda sentence: (len(sentence), sentence)) \
    .sortByKey(ascending=False) \
    .first()

print(f"The longest line:\n{longest_line[1]}\nWith character count: {longest_line[0]}")
