#!/usr/bin/python
import random

li = []

for i in range(1000):
	for j in range(i%7):
		li.append(i)


print(len(li))
print ("id(integer) num(integer)")

random.shuffle(li)

for i in range(len(li)):
	print li[i], i

