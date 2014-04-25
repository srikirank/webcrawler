#!/bin/bash

# make sure you have finish practice 1 and project 1
# echo "make sure you have finish practice 1 and project 1, otherwise press control+C"
# sleep 5


# clean existing compiled class
echo "Clean built java class and jar"
ant clean

# compile your code and shows errors if any
echo "Compiling source code with ant"
ant

if [ -f dist/lib/WebCrawling.jar ]
then
    echo "Source code compiled!"
else
    echo "There may be errors in your source code, please check the debug message."
    exit 255
fi

echo "Copy dist/lib/WebCrawling.jar file to hadoop lib under $HADOOP_INSTALL/lib/"
cp dist/lib/WebCrawling.jar $HADOOP_INSTALL/lib/

if [ -f $HADOOP_INSTALL/lib/WebCrawling.jar ]
then
    echo "File copied!"
else
    echo "There may be errors when copying file, please check if directory /root/software/hadoop-1.1.2/lib exists."
    exit 254
fi

export WC_CLASSPATH="/Users/sri/Development/Java/classpath"
export LIBJARS="$WC_CLASSPATH"/jsoup-1.7.3.jar
export HADOOP_CLASSPATH=`$HBASE_HOME/bin/hbase classpath`
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:`echo "$LIBJARS" | sed s/,/:/g`
#echo "export HADOOP_CLASSPATH=`/root/software/hbase-0.94.7/bin/hbase classpath`" >> ~/.bashrc
#source ~/.bashrc

# run wordcount
#hadoop jar $HADOOP_INSTALL/lib/WebCrawling.jar edu.wc.WebCrawler -libjars "$LIBJARS"
hadoop jar $HADOOP_INSTALL/lib/WebCrawling.jar edu.wc.PageRank -libjars "$LIBJARS" $1
