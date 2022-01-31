#!/bin/bash
shopt -s lastpipe
set -x #echo on

expno=1000000
nsb=10k
npb=10k

for nth in 3 6
do
	for intype in STR_TREE QUAD_TREE HPR_TREE MULTILAYER_GRID
	do
		java -cp GeospatialFiltering-1.0-SNAPSHOT.jar:lib/* hr.fer.retrofit.geofil.centralized.processing.ReplicatingProcessor $((expno++)) $nsb $npb $intype $nth
    	done
done