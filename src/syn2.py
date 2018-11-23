#!/usr/bin/python
import random

li = []

for i in range(300):
	for j in range(i):
		li.append(i)


print ("id(integer) num(integer)")

random.shuffle(li)

for i in range(len(li)):
	print len(li)-i-1, li[i]
