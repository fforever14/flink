#!/bin/bash

# 这个脚本允许提交一个sql文件到Flink集群中
# 在启动过程中, 脚本首先会启动一个Flink Yarn Session, 通过参数可指定所使用的资源
# 然后通过启动后的applicationId, 将SQL文件通过sql-client.sh提交到Flink Yarn Session中
# 需要传入的参数: yarn 任务名称  内存配置(可选)  执行的sql文件路径 额外的依赖
# 需要输出的数据: application_id job_id

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path
bin=`dirname "$target"`
export HADOOP_CLASSPATH=$(hadoop classpath)
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_CONF_DIR=/etc/hive/conf

# get flink config
. "$bin"/config.sh

usage() {
    echo "Usage: $0 [options] -nm [Job Name] --file [SQL File]"
    echo
    echo "   --jm         memory of job manager"
    echo
    echo "   --tm         memory of task manager"
    echo
    echo "   --queue      yarn queue name"
    echo
    echo "   --slot       slot number of flink yarn cluster"
    echo
    echo "   --file       sql file to run"
    echo
    echo "   --lib        external lib directory to import to job"
    echo
    echo "   -h|--help    print this help message"
    echo
    echo "   --nm         Yarn Job Name and Flink Job Name If Run As Once"
    echo
    echo "   --runAsOnce  run multiply insert as one job"
    exit 1
}

if [ $# -eq 0 ]; then
  usage
fi

ARGS=`getopt -a -u -o h -l help,jm:,nm:,queue:,tm:,slot:,file:,runAsOnce,lib: -- "$@"`
eval set -- "${ARGS}"
while true
do
  case "$1" in
  --jm)
    job_memory="$2"
    shift
    ;;
  --nm)
    job_name="$2"
    shift
    ;;
  --queue)
    queue_name="$2"
    shift
    ;;
  --slot)
    slot_number="$2"
    shift
    ;;
  --tm)
    task_memory="$2"
    shift
    ;;
  -h|--help)
    usage
    ;;
  --file)
    sql_file="$2"
    shift;
    ;;
  --runAsOnce)
    runAsOnce="true"
    ;;
  --lib)
    lib_dir="$lib_dir -l $2"
    shift;
    ;;
  --)
    shift
    break
    ;;
  esac
shift
done


if [ -z "$job_name" ]; then
  echo "job name not set, check help"
  usage
  exit 1
else
  job_name_conf="-nm $job_name"
  flink_job_name_conf="-jnm $job_name"
fi

if [[ -z "$sql_file" || ! -e "$sql_file" ]]; then
  echo "sql file can not find, check help"
  usage
  exit 1
else
  sql_file_conf="-f $sql_file"
fi

# 处理yarn-session所使用的参数
if [ -z "$job_memory" ]; then
  job_memory_conf="-jm 1024"
else
  job_memory_conf="-jm $job_memory"
fi

if [ -n "$queue_name" ]; then
  queue_name_conf="-qu $queue_name"
fi

if [ -n "$slot_number" ]; then
  slot_number_conf="-s $slot_number"
fi

if [ -z "$task_memory" ]; then
  task_memory_conf="-tm 2048"
else
  task_memory_conf="-tm $task_memory"
fi


# 启动yarnsession
echo "start yarn session..."
yarn_log=/tmp/flink-sql-yarnsession-start-$(date +%Y%m%d%H%M%S).log
$bin/yarn-session.sh -d $job_memory_conf $job_name_conf $queue_name_conf $slot_number_conf $task_memory_conf > $yarn_log

application_id=`cat $yarn_log | grep application | awk '$2=="yarn" {print $5}' | head -n 1`
web_url=`cat $yarn_log | grep "Found Web Interface" | awk '{print $10}'`

if [[ -z "$application_id" ]]; then
  echo "yarn session start failed, please check log $yarn_log"
else
  echo "start yarn session success"
  rm -rf $yarn_log
fi

# 提交Flink SQL Job
echo "start submit sql job to $application_id ..."
if [ -n "$runAsOnce" ]; then
  run_once_conf="-once"
fi

flink_job_log=/tmp/flink-sql-job-start-$(date +%Y%m%d%H%M%S).log
$bin/sql-client.sh embedded -d $FLINK_CONF_DIR/sql-client-hive.yaml -id $application_id $lib_dir $run_once_conf $sql_file_conf $flink_job_name_conf > $flink_job_log 2>&1

job_id=`cat $flink_job_log | grep "deploy job success" | awk '{print $4}'`

if [[ -z "$job_id" ]]; then
  echo "submit job faild, please check log $flink_job_log"
  echo "stop yarn session..."
  echo "stop" | $bin/yarn-session.sh -id $application_id
else
  echo "submit job success, application_id: $application_id job_id: $job_id web_url: $web_url"
  rm -rf $flink_job_log
fi

# 执行完成
echo "finish!"


