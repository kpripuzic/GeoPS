#!/bin/bash
shopt -s lastpipe
set -x #echo on

expno=300000
nrp=100000 #10000
nsb=10k
npb=100k #10k
npa=100
ncj=32 #4
mrp=64

for tex in balanced1 balanced2 balanced4 balanced8 balanced16 balanced
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

#    	for grtype in QUADTREE KDBTREE 
#	do
#    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geofil.distributed.spark.SpsProcessorSedonaJoin GeospatialFiltering-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
#    	done
#    	
#    	for intype in STR_TREE QUAD_TREE HPR_TREE 
#    	do
#    		for grtype in QUADTREE KDBTREE 
#	    	do	
#	    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geofil.distributed.spark.SpisProcessorSedonaJoin GeospatialFiltering-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp
#	    	done
#    	done

done
