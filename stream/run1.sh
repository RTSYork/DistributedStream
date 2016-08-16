#! /bin/bash

taskset -c 1-15 java8 -Xm{x,s}14G -XX:+UseNUMA -Djava.library.path=. -Djava.util.concurrent.ForkJoinPool.common.parallelism=14 $*
