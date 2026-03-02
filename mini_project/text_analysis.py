from pyspark import SparkContext
import re
import sys

sc = SparkContext("local[*]", "Sherlock Holmes Analysis")

try:
    rdd = sc.textFile("mini_project/data/sherlock_holmes.txt")
    whole_text_rdd = sc.wholeTextFiles("mini_project/data")

    def show_separator():
        print("\n", "-" * 80, "\n")

    def show_first_n_rows(rdd, n):
        for row in rdd.take(n):
            print(row) # Add single quotes around the String row to make it clear that it is a String value being displayed

    total_errors = sc.accumulator(0)

    # Function to filter out bad lines and add to the accumulator
    def remove_empty_lines(line):
        if line == "":
            total_errors.add(1)
            return False
        return True



    show_separator()



    # Task 1
    print("Task 1: Text Normalization\n")

    def normalize_text(line):
        # Use regular expression to get a list of all substrings including one or more alphabetic characters
        # Join them back together into a new string using a space character as the separator
        # Return the new line converted to lowercase
        if line == "":
            total_errors.add(1) # Log to accumulator empty line found
        matches = re.findall(r"[a-zA-Z]+", line) # use [a-zA-Z0-9]+ if you want to include numbers. [a-zA-Z'-]+ may also be useful to keep contractions and hyphenated words together
        alpha_line = " ".join(matches)
        return alpha_line.lower()

    normalized_rdd = rdd.map(lambda line: normalize_text(line))
    show_first_n_rows(normalized_rdd, 15)



    show_separator()



    # Task 2
    print("Task 2: Word Tokenization\n")


    # Splitting around white space and flattening will yield a list of words
    tokenized_rdd = normalized_rdd.flatMap(lambda line: line.split()) \
        .map(lambda line: line.strip()) \
        .filter(remove_empty_lines) # log empty lines and filter them out
    show_first_n_rows(tokenized_rdd, 15)



    show_separator()



    # Task 3
    print("Task 3: Filtering Stopwords\n")

    # From google search of common english stopwords
    stopwords = set(['i', 'me', 'my', 'myself', 'we', 'our', 'ours', 'ourselves', 'you', 'your', 'yours', 'yourself', 'yourselves', 'he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'hersheself', 'it', 'its', 'itself', 'they', 'them', 'their', 'theirs', 'themselves', 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those', 'am', 'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing', 'a', 'an', 'the', 'and', 'but', 'if', 'or', 'because', 'as', 'until', 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into', 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down', 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here', 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more', 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so', 'than', 'too', 'very', 's', 't', 're', 'can', 'will', 'just', 'don', 'should', 'now'])

    # Broadcast the set of stopwords
    # Using the tokenized rdd, filter out any stopwords
    # Unpersist the stopwords since they are no longer being used
    bc_stopwords = sc.broadcast(stopwords) # O2

    # processing function added to log empty lines
    def process_stopwords(word):
        if word == "":
            total_errors.add(1)
            return False
        return word not in bc_stopwords.value

    significant_rdd = tokenized_rdd.filter(process_stopwords)
    show_first_n_rows(significant_rdd, 15)
    bc_stopwords.unpersist() # O2



    show_separator()



    # Task 4 
    print("Task 4: Character Counting\n")

    # The requirement says counting every single character, so I will use the first rdd
    # Replace each line with its length and then sum the lengths. This automatically accounts
    # for empty lines
    character_count = rdd.filter(remove_empty_lines) \
        .map(lambda line: len(line)) \
        .reduce(lambda x, y: x + y)
    print(f"Number of characters: {character_count}")

    # Below solves the description given in the example

    # First split each line into lists of characters
    # Then map all alphabetical or space characters to the value 1, other characters to 0
    # And finally sum the values to get the total count
    character_count = rdd.filter(remove_empty_lines) \
        .flatMap(lambda word: list(word)) \
        .map(lambda letter: 1 if letter.isalpha() or letter.isspace() else 0) \
        .reduce(lambda x, y: x + y)
    print(f"Number of letters and spaces: {character_count}")



    show_separator()



    print("Task 5: Line Length Analysis\n")



    chapters = set(['ADVENTURE I. A SCANDAL IN BOHEMIA', 'ADVENTURE II. THE RED-HEADED LEAGUE', 'ADVENTURE III. A CASE OF IDENTITY', 'ADVENTURE IV. THE BOSCOMBE VALLEY MYSTERY', 'ADVENTURE V. THE FIVE ORANGE PIPS', 'ADVENTURE VI. THE MAN WITH THE TWISTED LIP',
        'VII. THE ADVENTURE OF THE BLUE CARBUNCLE', 'VIII. THE ADVENTURE OF THE SPECKLED BAND', "IX. THE ADVENTURE OF THE ENGINEER'S THUMB", 'X. THE ADVENTURE OF THE NOBLE BACHELOR', 'XI. THE ADVENTURE OF THE BERYL CORONET',
        'XII. THE ADVENTURE OF THE COPPER BEECHES'])
    # This chapter_pattern captures any occurrence of one of the chapter names
    chapter_pattern = "(" + "|".join(re.escape(name.upper()) for name in chapters) + ')'
    bc_chapters = sc.broadcast(chapter_pattern)

    # First split around roman numerals since they interfere with splitting around the period to get sentences
    # Then split around white space preceded by punctuation or quotation marks excluding formal abbreviations like 
    # Mr., Mrs., Dr. and that is followed by an uppercase letter or quotation mark to start the next sentence.
    sentences = whole_text_rdd.flatMap(lambda pair: re.split(r'\r\n\s*(?:III|II|I|IV|V|VIII|VII|VI|IX|X|XII|XI)\.', pair[1])) \
        .filter(remove_empty_lines) \
        .flatMap(lambda line: re.split(r'(?<![A-Z]r\.)(?<![A-Z]rs\.)(?<![A-Z]\.)(?<=[.?!\'"])\s+(?=[A-Z"\'])', line)) \
        .map(lambda sentence: sentence.replace('\r\n', ' ').strip()) \

    # Map each sentence to a pair RDD containing its length followed by its text
    # Sort by the sentence length in descending order and get the first row to 
    # access the longest sentence's length and text.
    longest_line = sentences.map(lambda sentence: (len(sentence), sentence)) \
        .sortByKey(ascending=False) \
        .first()

    print(f"The longest sentence:\n{longest_line[1]}\nWith character count: {longest_line[0]}")

    # Match sections of the text that are between pairs of quotation marks followed white space. 
    # The space is important to not count contractions like "it's".
    # I found that some lines are like: "I answered that I had not.
    # And followed by an empty line, and then more of what they say. I couldn't figure out how to address this using Spark
    dialogue = whole_text_rdd.flatMap(lambda line: re.findall(r'"[^"]*["(\n\n)]\s+', line[1])) \
        .filter(remove_empty_lines) \
        .map(lambda line: line.replace("\r\n", " ").strip())

    longest_dialogue = dialogue.map(lambda line: (len(line), line)) \
        .sortByKey(ascending=False) \
        .first()

    print(f"\n\nThe longest quote:\n{longest_dialogue[1]}\nWith character count: {longest_dialogue[0]}")



    show_separator()



    print("Task 6: Word Search (Filtering)\n")
    watson_sentences = sentences.filter(lambda sentence: "watson" in sentence.lower())
    print("\n------------Sentences containing Watson------------")
    for sentence in watson_sentences.take(5):
        print(sentence, "\n")

    watson_dialogue = dialogue.filter(lambda line: 'watson' in line.lower())
    print('\n------------Dialogue containing Watson------------')
    for line in watson_dialogue.take(5):
        print(line, "\n")



    show_separator()


    # Task 7
    print("Task 7: Unique Vocabulary Count\n")

    # From the tokenized rdd, get distinct values and then count
    unique_count = tokenized_rdd.distinct().count()
    print(f"{unique_count} words are used")



    show_separator()



    # Task 8
    print("Task 8: Top 10 Frequent Words\n")

    def get_and_show_top_ten_most_used_words(word_rdd):
        # From the rdd, make (word, 1) pair RDDs
        # reduce by key to get a list of (word, frequency) pair RDDs
        # Sort by descending frequency and take the top 10
        most_used_rdd = word_rdd.filter(remove_empty_lines) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda x, y: x + y) \
            .sortBy(lambda pair: pair[1], ascending=False)
        
        top_ten_most_used_rdd = most_used_rdd.take(10)

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
        
        return most_used_rdd

    print("Top ten most used words (including stop words)")
    most_used_words = get_and_show_top_ten_most_used_words(tokenized_rdd)

    print("Top ten most used words (excluding stop words)")
    get_and_show_top_ten_most_used_words(significant_rdd)



    show_separator()



    print("Task 9: Sentence Start Distribution\n")
    start_distribution = sentences.filter(remove_empty_lines) \
        .map(lambda sentence: (sentence.split()[0].lower(), 1)) \
        .reduceByKey(lambda x, y: x + y) 
    for entry in start_distribution.take(10):
        print(entry)



    show_separator()



    # Task 10
    print("Task 10: Average Word Length\n")

    # Using the tokenized RDD, map each word to a pair of (word length, 1)
    # Aggregate to sum word lengths and word counts
    total_length_and_word_count = tokenized_rdd.filter(remove_empty_lines) \
        .map(lambda word: (len(word), 1)) \
        .reduce(lambda pair1, pair2: (pair1[0] + pair2[0], pair1[1] + pair2[1]))
    print(f"The average word length is {total_length_and_word_count[0] / total_length_and_word_count[1]}")



    show_separator()



    # Task 11
    print("Task 11: Distribution of Word Lengths\n")
    # Using the tokenized RDD, map each word to a pair of (word length, 1)
    # Reduce by key to sum frequencies of each word length
    word_length_distribution = tokenized_rdd.filter(remove_empty_lines) \
        .map(lambda word: (len(word), 1)) \
        .reduceByKey(lambda x, y: x + y) \
        .sortByKey()
    show_first_n_rows(word_length_distribution, 15)



    show_separator()



    print("Task 12: Specific Chapter Extraction\n")
    chapters_rdd = whole_text_rdd.flatMap(lambda pair: re.split(bc_chapters.value, pair[1])[1:]) \
        .filter(remove_empty_lines) \
        .map(lambda text: text.replace("\r\n", " ").strip()) \
        .zipWithIndex() \
        .map(lambda x: (x[1] // 2, x[0])) \
        .groupByKey().mapValues(list) \
        .map(lambda pair: "\n".join(pair[1]))
    for chapter in chapters_rdd.take(5):
        print(chapter[:1024], "...\n\n")



    show_separator()



    print("O1: Found on lines 80-91\n")



    show_separator()



    print("O2: Global Error/Blank Line Counter (Accumulator)\n")
    print(f"The amount of empty lines encountered: {total_errors.value}")



    show_separator()



    print("O3: Word-Length Pairing (Pair RDD)\n")
    # Map each word from tokenized_rdd to a pairing showing its length
    word_length_pairing_rdd = tokenized_rdd.filter(remove_empty_lines) \
        .map(lambda word: (len(word), word)) 
    show_first_n_rows(word_length_pairing_rdd, 10)


    show_separator()



    print("O4: Character Frequency (Pair RDD / ReduceByKey)\n")
    # break each word in tokenized_rdd into characters and flatten
    # create (character, 1) pair RDD and reduceByKey to get frequency RDD
    character_frequency_rdd = tokenized_rdd.filter(remove_empty_lines) \
        .flatMap(lambda word: list(word)) \
        .map(lambda character: (character, 1)) \
        .reduceByKey(lambda x, y: x + y)
    show_first_n_rows(character_frequency_rdd, 10)



    show_separator()



    print("O5: Grouped Word List (GroupByKey)\n")
    # Make pair RDD mapping each word from tokenized words to a pair
    # with the first character as the key
    grouped_word_rdd = tokenized_rdd.filter(remove_empty_lines) \
        .map(lambda word: (word[0], word)) \
        .groupByKey()
    show_first_n_rows(grouped_word_rdd, 10)



    show_separator()



    print("O6: Loading from Multiple Sources\n")
    print("Achieved by wrapping content in try-except and giving the user instructions if the file isn't found.")



    show_separator()



    print("O7: Saving Analytics Results")
    most_used_words.coalesce(1).saveAsTextFile("mini_project/Holmes_WordCount_Results/")



    show_separator()



    bc_chapters.unpersist()
    sc.stop()    
except Exception as e:
    if "Input path does not exist" in str(e):
        print("Unable to locate mini_project/data/sherlock_holmes.txt\nPlease ensure the file exists and try again.")
    else:
        print(f"Something unexpected went wrong:\n{str(e)}")
