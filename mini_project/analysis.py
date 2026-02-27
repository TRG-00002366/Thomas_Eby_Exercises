from pyspark import SparkContext
import re

sc = SparkContext("local[*]", "Sherlock Holmes Analysis")
rdd = sc.textFile("mini_project/data/sherlock_holmes.txt")

def show_separator():
    print("\n", "-" * 80, "\n")

def show_first_n_rows(rdd, n):
    for row in rdd.take(n):
        print("'", row, "'", sep="") # Add single quotes around the String row to make it clear that it is a String value being displayed

show_separator()

# Task 1
print("Task 1: Text Normalization")

def normalize_text(line):
    # Use regular expression to get a list of all substrings including one or more alphabetic characters
    # Join them back together into a new string using a space character as the separator
    # Return the new line converted to lowercase
    matches = re.findall(r"[a-zA-Z]+", line) # use [a-zA-Z0-9]+ if you want to include numbers. [a-zA-Z'-]+ may also be useful to keep contractions and hyphenated words together
    alpha_line = " ".join(matches)
    return alpha_line.lower()

normalized_rdd = rdd.map(lambda line: normalize_text(line))
show_first_n_rows(normalized_rdd, 15)

show_separator()

# Task 2
print("Task 2: Word Tokenization")

# Splitting around white space and flattening will yield a list of words
tokenized_rdd = normalized_rdd.flatMap(lambda line: line.split())
show_first_n_rows(tokenized_rdd, 15)

show_separator()

# Task 3
print("Task 3: Filtering Stopwords")

# From google search of common english stopwords
stopwords = set(['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'hersheself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 're', 'can', 'will', 'just', 'don', 'should', 'now'])

# Broadcast the set of stopwords
# Using the tokenized rdd, filter out any stopwords
# Unpersist the stopwords since they are no longer being used
bc_stopwords = sc.broadcast(stopwords)
significant_rdd = tokenized_rdd.filter(lambda word: word not in stopwords)
show_first_n_rows(significant_rdd, 15)
bc_stopwords.unpersist()

show_separator()

# Task 4 
print("Task 4: Character Counting")

# The requirement says counting every single character, so I will use the first rdd
# Replace each line with its length and then sum the lengths. This automatically accounts
# for empty lines
character_count = rdd.map(lambda line: len(line)) \
    .reduce(lambda x, y: x + y)
print(f"Number of letters and spaces: {character_count}")

# Below solves the description given in the example

# First split each line into lists of characters
# Then map all alphabetical or space characters to the value 1, other characters to 0
# And finally sum the values to get the total count
# character_count = rdd.flatMap(lambda word: list(word)) \
#     .map(lambda letter: 1 if letter.isalpha() or letter.isspace() else 0) \
#     .reduce(lambda x, y: x + y)

show_separator()

# Task 5
print("Task 5: Line length analysis")

# Following the requirement I'm going to first find the longest line in the text and its character count
# The example seems practically impossible with RDDs

# Replace lines with a tuple pair storing the line followed by its length
# Sort the lines so that the first line has the greatest length, and get it
# longest_line = rdd.map(lambda line: (line, len(line))) \
#     .sortBy(lambda pair: pair[1], ascending=False) \
#     .first()
whole_text_rdd = sc.wholeTextFiles("mini_project/data")

chapters = set(['I. A Scandal in Bohemia', 'II. The Red-headed League', 'III. A Case of Identity', 'IV. The Boscombe Valley Mystery', 'V. The Five Orange Pips', 'VI. The Man with the Twisted Lip',
    'VII. The Adventure of the Blue Carbuncle', 'VIII. The Adventure of the Speckled Band', "IX. The Adventure of the Engineer's Thumb", 'X. The Adventure of the Noble Bachelor', 'XI. The Adventure of the Beryl Coronet',
    'XII. The Adventure of the Copper Beeches'])

bc_chapters = sc.broadcast(chapters)

sentences = whole_text_rdd.flatMap(lambda pair: re.split(r'\r\n(\s)*(I.|II.|III.|IV.|V.|VI.|VII.|VIII.|IX.|X.|XI.|XII.)', pair[1])) \
    # .flatMap(lambda line: re.split(r'[.?!]', line)) \
    # .map(lambda sentence: sentence.replace('\r\n', ' ').strip())
print(sentences.take(5))
longest_line = ('asdlfk', 5)
print(f"The longest line:\n{longest_line[0]}\nWith character count: {longest_line[1]}")

show_separator()

# Task 6
print("Task 6: Word Search (Filtering)")

# Filter for lines containing "Watson" from the original rdd
watson_rdd = rdd.filter(lambda line: "Watson" in line)
show_first_n_rows(watson_rdd, 15)

show_separator()

# Task 7
print("Task 7: Unique Vocabulary Count")

# From the tokenized rdd, get distinct values and then count
unique_count = tokenized_rdd.distinct().count()
print(f"{unique_count} words are used")

show_separator()

# Task 8
print("Task 8: Top 10 Frequent Words")

# From the tokenized rdd, make (word, 1) pairs
# reduce by key to get a list of (word, frequency) pairs
# Sort by frequency descending and take the top 10
top_ten_most_used_rdd = tokenized_rdd.map(lambda word: (word, 1)) \
    .reduceByKey(lambda x, y: x + y) \
    .sortBy(lambda pair: pair[1], ascending=False) \
    .take(10)

# Get longest word length to format the output string, justifying each
# word to the right so that they effectively have the same length and 
# line up nicely
longest_word_in_list_length = 0
for word, _ in top_ten_most_used_rdd:
    if len(word) > longest_word_in_list_length:
        longest_word_in_list_length = len(word)

# Show the 10 most frequently used words along with their frequencies
for index, pair in enumerate(top_ten_most_used_rdd):
    print(f"{str(index + 1).rjust(2)}. {str(pair[0]).rjust(longest_word_in_list_length)}: {pair[1]}")

show_separator()

# Task 9
print("Task 9: Sentence Start Distribution")

# Using the normalized rdd which has lines of only words,
# Filter out any empty lines 
# Map each line to only contain its first word by splitting and accessing the first element in the resulting list
# Create (word, 1) pairs
# Reduce to sum the word frequencies
sentence_start_rdd = normalized_rdd.filter(lambda line: len(line) > 0) \
    .map(lambda line: (line.split()[0], 1)) \
    .reduceByKey(lambda x, y: x + y)
show_first_n_rows(sentence_start_rdd, 15)

show_separator()

# Task 10
print("Task 10: Average Word Length")

# Using the tokenized RDD, map each word to a pair of (word length, 1)
# Aggregate to sum word lengths and word counts
total_length_and_word_count = tokenized_rdd.map(lambda word: (len(word), 1)) \
    .reduce(lambda pair1, pair2: (pair1[0] + pair2[0], pair1[1] + pair2[1]))
print(f"The average word length is {total_length_and_word_count[0] / total_length_and_word_count[1]}")

# Task 11
print("Task 11: Distribution of Word Lengths")
# Using the tokenized RDD, map each word to a pair of (word length, 1)
# Reduce by key to sum frequencies of each word length
word_length_distribution = tokenized_rdd.map(lambda word: (len(word), 1)) \
    .reduceByKey(lambda x, y: x + y)
show_first_n_rows(word_length_distribution, 15)

show_separator()

print("Task 12: Specific Chapter Extraction")
# 