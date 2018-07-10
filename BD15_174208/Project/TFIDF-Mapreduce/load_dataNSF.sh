#!/bin/bash
#
# Script to download  dataset and load into HDFS.
#

#if file existes delete it

hadoop fs -rm -r /dataNSF

hadoop fs -mkdir  /dataNSF


for i in $(seq 1 3); do
wget http://archive.ics.uci.edu/ml/machine-learning-databases/nsfabs-mld/Part$i.zip
unzip -cq Part$i.zip 
done

for i in $(seq 1 3);do
	for j in $(seq 1990 2003); do
		for k in $(seq 10 99); do
if [ "$k" -lt 10 ]; then
	hadoop fs -put Part$i/awards_$j/awd_$j_0$k/.* /dataNSF
else
	hadoop fs -put Part$i/awards_$j/awd_$j_$k/.* /dataNSF
fi
		done
	done
done

