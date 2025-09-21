# Date of Birth: June 11, 2001

# This script performs two data analysis tasks using a basic, in-memory MapReduce paradigm.
# 1. Counts all words in a given text.
# 2. Counts non-English words in a separate text file, using pyspellchecker to identify them.

# --- Prerequisites ---
# You need to install the pyspellchecker library to run this script.
# You can install it using pip:
# pip install pyspellchecker

import re
from collections import defaultdict
from spellchecker import SpellChecker

# --- File Contents ---
# Simulating the contents of file.txt and file2.txt by using multi-line strings.
# You can replace this with file reading logic if you have physical files.

# Contents for the first part of the assignment.
file_1_content = """
The quick brown fox jumps over the lazy dog.
The dog barks at the fox, and the fox runs away.
The quick brown fox.
"""

# Contents for the second part of the assignment.
# This text includes some made-up, non-English words to demonstrate the functionality.
file_2_content = """
The wizard Gandalf cast a powerful spell, "Ignis Fatuus,"
and the dragon Smaug roared. In the city of Gondor,
the knight Aragorn met a goblin named Azog. The ancient
book mentioned a place called Khazad-dûm, and the prophecy was true.
"""


# --- MapReduce Functions ---

def mapper(text):
    """
    The mapper function takes a string of text, tokenizes it into words,
    and yields a key-value pair for each word.
    Key: The word (lowercase).
    Value: The number 1.
    """
    # Use a regular expression to find all words (alphanumeric sequences).
    words = re.findall(r'\b\w+\b', text.lower())
    for word in words:
        yield word, 1

def reducer(key, values):
    """
    The reducer function takes a key and a list of values,
    and aggregates them to produce a final result.
    Key: The word.
    Values: A list of counts (1s from the mapper).
    Returns: The word and its total count.
    """
    return key, sum(values)


# --- Part 1: Word Count for file.txt ---

print("--- Part 1: Word Count for file.txt ---")

# 1. Map Phase
# Apply the mapper function to the content of file_1_content.
mapped_results = []
for line in file_1_content.strip().split('\n'):
    mapped_results.extend(list(mapper(line)))

# 2. Shuffle & Sort Phase (Group by key)
# Group all the values (1s) by their corresponding keys (words).
shuffled_data = defaultdict(list)
for key, value in mapped_results:
    shuffled_data[key].append(value)

# 3. Reduce Phase
# Apply the reducer function to each group of words.
reduced_results_part1 = dict(reducer(key, values) for key, values in shuffled_data.items())

# Sort the results for better readability and print.
sorted_results_part1 = sorted(reduced_results_part1.items())
for word, count in sorted_results_part1:
    print(f"'{word}': {count} times")
print("\n" + "="*50 + "\n")


# --- Part 2: Non-English Word Count for file2.txt ---

print("--- Part 2: Non-English Word Count for file2.txt ---")

def non_english_mapper(text):
    """
    The mapper function for non-English words. It uses a spell checker
    to identify words that are not in the English dictionary and
    yields a key-value pair for each non-English word.
    """
    spell = SpellChecker()
    words = re.findall(r'\b\w+\b', text.lower())
    
    # The `unknown` method returns a set of all words that are not in the dictionary.
    non_english_words = spell.unknown(words)
    
    for word in words:
        if word in non_english_words:
            yield word, 1

# 1. Map Phase
# Apply the non_english_mapper to the content of file_2_content.
mapped_results = []
for line in file_2_content.strip().split('\n'):
    mapped_results.extend(list(non_english_mapper(line)))

# 2. Shuffle & Sort Phase (Group by key)
# Group the non-English words by their corresponding keys.
shuffled_data = defaultdict(list)
for key, value in mapped_results:
    shuffled_data[key].append(value)

# 3. Reduce Phase
# Apply the reducer function to each group of non-English words.
reduced_results_part2 = dict(reducer(key, values) for key, values in shuffled_data.items())

# Sort the results and print.
sorted_results_part2 = sorted(reduced_results_part2.items())
for word, count in sorted_results_part2:
    print(f"'{word}': {count} times")
print("="*50 + "\n")

