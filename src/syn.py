#!/usr/bin/python
import random

li = []

for i in range(300):
	for j in range(i):
		li.append(i)


print ("num(integer) id(integer)")

random.shuffle(li)

for i in range(len(li)):
	print li[i], i
