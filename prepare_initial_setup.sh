#!/bin/bash

echo "Script for preparing initial file setup"
echo "------------------------------------------------"

echo "Checking if README.md exists in the current working dir -->"
if test -f "README.md"; then
    echo "README.md exists"
else
        echo "Copying readme file from the storage"
        cp /home/s58/initial_files/README.md .
        if [ $? -eq 0 ]; then echo "OK"; else echo "Problem copying README.md file"; exit 1; fi
fi
echo "------------------------------------------------"

echo "Checking if LICENCE.md exists in the current working dir -->"
if test -f "LICENCE.md"; then
    echo "LICENCE.md exists"
else
        echo "Copying LICENCE.md file from the storage"
        cp /home/s58/initial_files/LICENCE.md .
        if [ $? -eq 0 ]; then echo "OK"; else echo "Problem copying LICENCE.md file"; exit 1; fi
fi
echo "------------------------------------------------"

echo "Checking if config.ini exists in the current working dir -->"
if test -f "config.ini"; then
    echo "config.ini exists"
else
        echo "Copying readme file from the storage"
        cp /home/s58/initial_files/config.ini .
        if [ $? -eq 0 ]; then echo "OK"; else echo "Problem copying config.ini file"; exit 1; fi
fi
echo "------------------------------------------------"

echo "Checking if .gitignore exists in the current working dir -->"
if test -f ".gitignore"; then
    echo ".gitignore exists"
else
        echo "Copying .gitignore file from the storage"
        cp /home/s58/initial_files/.gitignore .
        if [ $? -eq 0 ]; then echo "OK"; else echo "Problem copying .gitignore file"; exit 1; fi
fi
echo "------------------------------------------------"

echo "Initial setup check finished! You can start coding!"

