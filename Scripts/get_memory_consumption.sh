#!/bin/bash
for id in $(cat appids.txt); do echo -ne $id" " && ./max_memory_on_workers.sh $id; done
