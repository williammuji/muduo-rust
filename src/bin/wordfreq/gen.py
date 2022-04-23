#!/usr/bin/python3

import random

words = 1000000
word_len = 5
alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-'

output = open('/tmp/random_words', 'w')
for x in range(words):
	arr = [random.choice(alphabet) for i in range(word_len)]
	word = ''.join(arr)
	output.write(word)
	output.write('\n')
