#!/usr/bin/env bash
# This program should take a fileIn as the first parameter:
# It takes the input log file that has the same format as the `access_log` file and maps it to a CSV format.
# The CSV format is:
# Client,Time,Type,Path,Status,Size
#
# The program should not create a CSV file.
# This can be done by piping the output to a file.
# Example: `./logToCSV access_log > output.csv`
# It could take some time to convert all of the `access_log` file contents. Consider using a small subset for testing.

# line="h24-71-236-129.ca.shawcable.net - - [11/Mar/2004:12:28:51 -0800] \"GET /icons/gnu-head-tiny.jpg HTTP/1.1\" 304 -"
# echo $line | sed -E 's/ - - /,/;s/\[//;s/\]//;s/ "/,/;s/ \//,/;s/ ([A-Z]|[a-z])+\/[0-9].[0-9]" /,/;s/ ([0-9])/,&/;s/, /,/;s/[0-9][0-9][0-9] -/&???/;s/ -\?\?\?/,-/'
sed -E 's/ - - /,/;s/\[//;s/\]//;s/ "/,/;s/ \//,\//;s/ ([A-Z]|[a-z])+\/[0-9].[0-9]" /,/;s/ ([0-9])/,&/;s/, /,/;s/[0-9][0-9][0-9] -/&???/;s/ -\?\?\?/,-/;s/ -[0-9][0-9][0-9][0-9]//' $1

# sed -E 's/ - - /,/        separate Client and Time
#         ;s/\[//;s/\]//    Remove [] from Time
#         ;s/ "/,/          separate Time and Type
#         ;s/ \//,\//       separate Type and Path
#         ;s/ ([A-Z]|[a-z])+\/[0-9].[0-9]" /,/      remove Version
#         ;s/ ([0-9])/,&/;s/, /,/   separate Status and Path
#         ;s/[0-9][0-9][0-9] -/&???/;s/ -\?\?\?/,-/    separate Status and Size
#         ;s/ -[0-9][0-9][0-9][0-9]//'  Remove timezone
# fileIn=$1

# client=$(cat $fileIn | cut -d ' ' -f 1)

# time=$(cat $fileIn | cut -d ' ' -f 4 | tr -d '[')

# type=$(cat $fileIn | cut -d '"' -f 2 | cut -d ' ' -f 1)

# #path=$(cat $fileIn | tr '"-"' '"- - -"' | cut )
# #echo "$type"


# #helper=$(cat $1 | grep ".*telia.*")
# #echo "$helper"
# result=$(cat $fileIn | sed -e 's/"-"/"- - -"/' | cut -d ' ' -f 1,4,6,7,9,10 | tr -d '[' | tr -d '"' | tr ' ' ',')

# echo "$result" > output.csv
# echo $(cat "$1" | wc -l)
# echo "$result" | wc -l
