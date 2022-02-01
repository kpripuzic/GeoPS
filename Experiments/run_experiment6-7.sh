#!/bin/bash
shopt -s lastpipe
set -x #echo on

expno=600000
nrp=10000
nsb=10k
ncj=4
mrp=64
npa=100
tex=balanced

for npb in 10k-0 10k-0.25 10k-0.5 10k-0.75 10k-1
do
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

expno=700000
npb=10k
for tso in COVERED_BY COVERS CONTAINS CROSSES EQUALS INTERSECTS OVERLAPS TOUCHES WITHIN DISJOINT
do
	for grtype in QUADTREE KDBTREE 
	do
  		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.SpsProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp -tso $tso
    	done
	    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		for grtype in QUADTREE KDBTREE 
	    	do	
	    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.SpisProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp -tso $tso
	    	done
    	done

	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		for grtype in QUADTREE KDBTREE 
	    	do	
	    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.RispsProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -tgd $grtype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp -tso $tso
	    	done
    	done	
	    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
    	do
    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.RihpsProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -npa $npa -ncj $ncj -nrp $nrp -mrp $mrp -tso $tso
    	done
    	
    	for intype in STR_TREE QUAD_TREE HPR_TREE 
	do
    		spark-submit --jars $(echo lib/*.jar | tr ' ' ',') --driver-memory 32G --master yarn --class hr.fer.retrofit.geops.distributed.spark.RisProcessor GeoPS-1.0-SNAPSHOT.jar -nex $((expno++)) -nsb $nsb -npb $npb -tex $tex -tix $intype -ncj $ncj -nrp $nrp -mrp $mrp -tso $tso
    	done
done
