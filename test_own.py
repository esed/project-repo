import os

from naked import *

# Testing if configuration file exists on disk in the current working directory
print("----------")
print("Checking if naked file exists -->")
assert os.path.isfile("naked.py") == True
print("OK")
print("----------")

print("Naked file existance test DONE -> ALL OK")
print("----------------------------------------")
