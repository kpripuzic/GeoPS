#!/bin/bash
for host in $(cat workers.txt)
do 
       	ssh $host 'cat /var/log/hadoop-yarn/hadoop-cmf-yarn-NODEMANAGER-'$host'.streamslab.fer.hr.log.out |grep '$1'|grep "Max memory usage of"|grep -v "MB of"'
done
