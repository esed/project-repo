#!/bin/bash

echo "Script for preparing a readme file"
echo "------------------------------------------------"

echo "Checking if README.md exists in the current working dir -->"
if test -f "README.md"; then
    echo "exists"
else
	echo "Copying readme file from the storage"
	cp home/s58/readme_file/README.md .
	if [ $? -eq 0 ]; then echo "OK"; else echo "Problem copying README.md file"; exit 1; fi
fi
echo "------------------------------------------------"
echo "README file prepared! You are ready to code."
