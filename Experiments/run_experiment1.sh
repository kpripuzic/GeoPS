#!/bin/bash
shopt -s lastpipe
set -x #echo on

expno=100000
nrp=10000
nsb=10k
npb=10k
ncj=4
mrp=64
npa=100

for tex in oversized fat balanced small tiny
do
	for grtype in QUADTREE KDBTREE 
	do
  		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geofil.distributed.spark.SpsProcessor GeospatialFiltering-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
    	done
	    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		for grtype in QUADTREE KDBTREE 
	    	do	
	    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geofil.distributed.spark.SpisProcessor GeospatialFiltering-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
	    	done
    	done

	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		for grtype in QUADTREE KDBTREE 
	    	do	
	    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geofil.distributed.spark.RispsProcessor GeospatialFiltering-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
	    	done
    	done	
	    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geofil.distributed.spark.RihpsProcessor GeospatialFiltering-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
    	done
    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
	do
    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geofil.distributed.spark.RisProcessor GeospatialFiltering-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -ncj $ncj -nrp $nrp -mrp $mrp
    	done
done
