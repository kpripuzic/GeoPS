#!/bin/bash
/bin/bash ./max_memory_on_workers_raw.sh $1 |sort -rn -k 14 |head -1| awk '{print $14, $15}'