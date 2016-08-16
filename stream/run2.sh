#! /bin/bash

java8 -Xm{x,s}14G -XX:+UseNUMA -Djava.library.path=. -Djava.util.concurrent.ForkJoinPool.common.parallelism=14 -Djava.util.concurrent.ForkJoinPool.common.threadFactory=MyThreadFactory $*
