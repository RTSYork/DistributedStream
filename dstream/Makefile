SRCS := $(wildcard dstream/*.java util/*.java)
OBJS := $(SRCS:.java=.class)

HADOOP_CP := $$HADOOP_HOME/hadoop-core-1.2.1.jar:$$HADOOP_HOME/lib/commons-logging-1.1.1.jar:$$HADOOP_HOME/lib/commons-configuration-1.6.jar:$$HADOOP_HOME/lib/commons-lang-2.4.jar
SRCS := $(filter-out dstream/MPICommunicator.java,$(SRCS))
OBJS := $(SRCS:.java=.class)
MPJ_CP := $$MPJ_HOME/lib/mpj.jar
JC := javac -Xdiags:verbose -cp .:$(HADOOP_CP):$(MPJ_CP)
PROG := $$MPJ_HOME/bin/mpjrun.sh
CLUSTER := -dev hybdev
CMD := -cp .:$(HADOOP_CP) dstream.ComputeNode WordCount input/input.txt

.PHONY: lib
lib: $(SRCS) HDFSConnComponents.java HDFSPageRank.java HDFSWordCount.java HDFSGrep.java HDFSSort.java HDFSBayes.java
	$(JC) $^

%.class: %.java
	$(JC) $^
