#!/bin/bash
shopt -s lastpipe
set -x #echo on

expno=500000
nrp=10000
npb=10k
ncj=4
mrp=64
tex=balanced

for nsb in 100 1k 10k 100k 1M
do
	if [[ $nsb == 100 ]]
		then
			npa=20
		else
			npa=100
	fi
	
    	for grtype in QUADTREE KDBTREE 
	do
    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.SpsProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
    	done
    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		for grtype in QUADTREE KDBTREE 
	    	do	
	    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.SpisProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
	    	done
    	done

	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		for grtype in QUADTREE KDBTREE 
	    	do	
	    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.RispsProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
	    	done
    	done	
    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.RihpsProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
    	done

	for intype in STR_TREE QUAD_TREE HPR_TREE 
	do
    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.RisProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -ncj $ncj -nrp $nrp -mrp $mrp
    	done
done
