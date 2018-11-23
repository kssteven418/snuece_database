#!/usr/bin/python
import random

li = []

for i in range(1000):
	for j in range(i%5):
		li.append(i)


print ("num(integer) id(integer)")

random.shuffle(li)

for i in range(len(li)):
	print li[i], i
