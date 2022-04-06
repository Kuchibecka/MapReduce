#!/bin/bash
#####

### MapReduce run script

# 1) Get num of iterations and scale from cmd
# 2) Build project jar
# 3) Start Hadoop services
# 4) Clear local and hdfs space
# 5) Prepare Hadoop job
# 6) Run Hadoop job
# 7) Introduce results
# 8) Clear space and end work

#####

ITER="100000" # iterations number & scale (if not mentioned when executing script)
SCALE="d"

# input& output config
HADOOP_INPUT="input"
HADOOP_OUTPUT="output"
JAR="target/MapReduce-1.0-SNAPSHOT-jar-with-dependencies.jar"
RESULT="result.txt"


    ### Getting cmd arguments: amount of iterations & scale
    echo
    echo
    echo "-------------------------Getting cmd args-------------------------------"
    while [ -n "$1" ]
    do
        case "$1" in
                    "--iter")
                        shift 1
                        ITER="$1"
                        echo "ITER: "
                        echo $ITER;;

                    "--scale")
                        shift 1
                        SCALE="$1" # s, m, h, d
                        #
                        # check true scale
                        ok=0
                        for scale in s m h d
                        do
                            test "$SCALE" = "$scale" && ok="1"
                        done
                        #
                        test "$ok" = "0" && \
                            { exit 1; };;
        esac
        shift 1
    done

    echo
    echo "Arguments:"
    echo "ITER: "
    echo $ITER
    echo "SCALE"
    echo $SCALE
    echo
    ###

    ### Building project jar
    echo
    echo
    echo "-------------------------Building project jar---------------------------"
    mvn clean
    mvn compile
    mvn package
    test -f $JAR || \
            { echo "No jar file"; return 1; }
    ###

    ### Starting Hadoop services
    echo
    echo
    echo "-----------------------Starting Hadoop services-------------------------"
    /opt/hadoop-2.10.1/sbin/start-dfs.sh
    /opt/hadoop-2.10.1/sbin/start-yarn.sh
    hadoop dfsadmin -safemode leave
    jps
    ###

    ### Clearing hdfs and local input, output, result files
    echo
    echo
    echo "--------------------Clearing space for input & output-------------------"
    hdfs dfs -rm -r $HADOOP_OUTPUT
    hdfs dfs -rm -r $HADOOP_INPUT

    # clearing local input, output, result files
    rm -r $HADOOP_INPUT
    rm -r $HADOOP_OUTPUT
    rm $RESULT
    ###

    ### Preparing Hadoop job
    echo
    echo
    echo "-----------------Generating test data + preparing input-----------------"
    # generating test data
     sh generateData.sh $ITER
     res=$?
     while [ "$i" != "10" ]
       do
         if [[ $(($res)) -eq 1 ]]; then
           echo "data generated"
           break
         fi
         sleep 2
         i=$(( $i + 1 ))
       done

    # putting input directory to hdfs
    hdfs dfs -put $HADOOP_INPUT $HADOOP_INPUT
    ###

    ### Running Hadoop job
    echo
    echo
    echo "------------------------Running Hadoop job------------------------------"
    yarn jar $JAR $HADOOP_INPUT $HADOOP_OUTPUT $SCALE `realpath ./metricsRef.txt`
    ###

    ### Introducing results of work
    echo
    echo
    echo "---------------------------Result of work-------------------------------"
    hdfs dfs -ls $HADOOP_OUTPUT
    mkdir $HADOOP_OUTPUT
    hdfs dfs -text $HADOOP_OUTPUT/part-r-00000 &> $RESULT
    hdfs dfs -get $HADOOP_OUTPUT
    ###

    ### Cleaning and ending work
    echo
    echo
    echo "-------------------------Clearing and ending work-----------------------"
    echo
    echo -n "Hit enter to clear hdfs & local files, stop Hadoop and execute mvn clean";
    read;

    # clearing local & hdfs files
    hdfs dfs -rm -r $HADOOP_OUTPUT
    hdfs dfs -rm -r $HADOOP_INPUT
    rm -r $HADOOP_INPUT
    rm -r $HADOOP_OUTPUT
    rm $RESULT

    # stop Hadoop
    /opt/hadoop-2.10.1/sbin/stop-dfs.sh
    /opt/hadoop-2.10.1/sbin/stop-yarn.sh

    # mvn clean
    mvn clean
    ###

    exit 0
