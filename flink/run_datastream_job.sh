#!/bin/bash
set -eu

function start_mjjk_job(){
    if [ $# -ne 4 ]
    then
      echo "Format Error! e.g: 610 gz 2 16;"
      exit 1
    fi

    local cluster_id="$1"
    local pulsar_area="$2"
    local max_topic_seq="$3"
    local parallelism="$4"

    local topic_name=""
    for tpi in `seq 0 $max_topic_seq`; do topic_name="${topic_name}cdb-${pulsar_area}-topic-${cluster_id}0${tpi},"; done
    topic_name=${topic_name%?}

    cd /usr/local/flink/log
    nohup /usr/local/flink/bin/flink run -d -c org.datastream.job.DataStreamProcessingJob /usr/local/flink/jar/datastream-processing-main-1.0.jar -c /usr/local/flink/conf/datastream_job.conf -p ${parallelism} -pulsar_topics ${topic_name} >> /usr/local/flink/log/${topic_name}.log 2>&1 &
}

queue_jobs="610 gz 2 16;"

i=1
while((1==1))
do
    #seperate by ;
    split=`echo "${queue_jobs}"|cut -d ";" -f$i`
    if [ "$split" != "" ]
    then
            ((i++))
            cluster_id=`echo "$split" | cut -d " " -f1`
            pulsar_area=`echo "$split" | cut -d " " -f2`
            max_topic_seq=`echo "$split" | cut -d " " -f3`
            parallelism=`echo "$split" | cut -d " " -f4`
            topic_name="cdb-${pulsar_area}-topic-${cluster_id}0${max_topic_seq}"

            jobcount=`/usr/local/flink/bin/flink list 2>/dev/null | grep "$topic_name" | grep -v 'grep' | wc -l`
            echo "${pulsar_area} ${topic_name} jobcount:$jobcount"
            if [ "$jobcount" -eq 1 ]
            then
              jobid=`/usr/local/flink/bin/flink list 2>/dev/null | grep "$topic_name" | grep -v 'grep'| awk '{print $4}'`
              /usr/local/flink/bin/flink cancel "$jobid" 2>/dev/null
              echo "${pulsar_area} ${topic_name} jobid: $jobid canceled"
            elif [ "$jobcount" -gt 1 ]
            then
              echo "$topic_name has more than 1 job,pls check"
              break
            fi
            start_mjjk_job "${cluster_id}" "${pulsar_area}" "${max_topic_seq}" "${parallelism}"
            echo "${pulsar_area} ${topic_name} start"

            if [ $i -gt 100 ]
            then
                echo "add more then 100 jobs, pls check"
                exit 2
            fi
    else

            break
    fi
done


