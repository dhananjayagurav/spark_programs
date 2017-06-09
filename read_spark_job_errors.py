import re
import os
import subprocess

'''
Steps :
        1. Open the log file
        2. grep the line number from where log starts (Assumption : spark job logs are appended to same file)
        3. Traverse each line to collect
                a. required stats
                b. errors, exceptions etc
'''

#Find line from where latest logging started
log_file = "/tmp/log_file_path.log"

grep_command = "grep -irn \"Ivy Default Cache set\" "+log_file+" | tail -n 1"
lineno_string = subprocess.Popen(grep_command, stdout=subprocess.PIPE, shell=True).stdout.read()
split_line = lineno_string.split(":")

#Add latest logs to a temporary file
temp_file ="/tmp/temp_analysis.txt"
tail_command = "tail -n +"+split_line[0]+" "+log_file+" > "+temp_file
os.system(tail_command)

#Search the latest logs collected with the desired pattern
with open(temp_file,'r') as fp:
    for line in fp:
        line = line.rstrip()
        if re.search('Status : FAILED',line):
            print line
