
# mgf_tee
#
# echo message to stdout and stderr
# useful for coordinating output in job's .out and .err
#
mgf_tee() {
  printf "$*\n" 
  printf "$*\n" 1>&2
  return 0
}
export -f mgf_tee

# mgf_date
#
# echo date and message to stdout and stderr
# useful for coordinating output in job's .out and .err
#
mgf_date() {
  printf "\n[$(date +'%D %T')] **** $*\n" 
  printf "\n[$(date +'%D %T')] **** $*\n" 1>&2
  return 0
}
export -f mgf_date

# mgf_section_name
#
# sets MGF_SECTION_NAME=cluster_process (formatted)
#
mgf_section_name() {
  local P0=${CLUSTER:-`date +%s | cut -c -6`}
  local P1=${PROCESS:-`date +%s | cut -c 7-`}
  P0=`printf "%07d" $P0`
  P1=`printf "%07d" $P1`
  export MGF_SECTION_NAME=${P0}_${P1}
  return 0
}
export -f mgf_section_name

# mgf_system
#
# print info about the system
#  -l long version
#  -v longer version
#
mgf_system() {

  local AL=""
  local AV=""
  local OPT OPTIND
  while getopts "lv" OPT; do
    case $OPT in
      l)
        AL=true;
        ;;
      v)
        AV=true;
        ;;
     esac
  done

  mgf_tee host=`hostname`
  mgf_tee OS=`cat /etc/redhat-release`
  mgf_tee uname=`uname -a`
  mgf_tee OSG_SITE_NAME=$OSG_SITE_NAME
  mgf_tee GLIDEIN_Site=$GLIDEIN_Site
  mgf_tee CLUSTER=$CLUSTER
  mgf_tee PROCESS=$PROCESS

  pwd=$PWD

  df -h

  grid-proxy-info

  if [ $AL$AV ]; then
    echo "PATH print ***************************"
    printenv PATH | tr ":" "\n"
    echo "LD_LIBRARY_PATH print ****************"
    printenv LD_LIBRARY_PATH | tr ":" "\n"
    echo "typeset print ****************"
    typeset
  fi

  if [ $AV ]; then

    echo "GLIBC versions ***********************"
    local LIBS=$(find /lib  -maxdepth 1 -type f -iname "libglib*")
    echo GLIBC versions in $LIBS
    strings $LIBS | grep GLIBC

    echo "cat /proc/cpuinfo ***********************"
    cat /proc/cpuinfo

    echo "rpm listing *******************"
    rpm -q -a

    local LIST LL

    echo "PATH full listing *******************"
    LIST=$(printenv PATH | tr ":" " ")
    for LL in $LIST
    do
      echo ">>>> $LL"
      ls -alR $LL
    done

    echo "LD_LIBRARY_PATH full listing *******************"
    LIST=$(printenv LD_LIBRARY_PATH | tr ":" " ")
    for LL in $LIST
    do
      echo ">>>> $LL"
      ls -alR $LL
    done

    echo "libs full listing *******************"
    LIST="/lib /lib64 /usr/lib /usr/lib64 /usr/local/lib /usr/local/lib64"
    for LL in $LIST
    do
      echo ">>>> $LL"
      ls -alR $LL
    done

  fi

  return 0
}
export -f mgf_system


# mgf_sam_start_consumer
#
# Starts a consumer for the sam project
# the job should have been submitted with sam settings
# you need to setup ifdh or setup a base release
# you need to setup sam_web_client
# requires SAM_PROJECT to be set (jobsub will do that)
# you may set SAM_FILE_LIMT
#
# return environmental SAM_CONSUMER_ID
#
# -v verbose
#
mgf_sam_start_consumer() {

  local AV=""
  local OPT OPTIND
  while getopts "v" OPT; do
    case $OPT in
      v)
        AV=true;
        ;;
     esac
  done

  if [ "`which samweb`" == "" ]; then
    mgf_date "ERROR mgf_sam_start_consumer - samweb_client not setup"
    return 1
  fi

  if [ ! $SAM_PROJECT ]; then
    mgf_date "ERROR in mgf_start_sam_consumer - SAM_PROJECT must be set to use this function"
    return 1
  fi

  # start the consumer with no local file
  export SAM_FILE=""

  # check that jobsub_client started a project
  [ $AV ] && echo SAM_PROJECT=$SAM_PROJECT

  # print a summary of the project
  if [ $AV ]; then
     if ! samweb project-summary $SAM_PROJECT ; then
       mgf_date "ERROR mgf_start_sam_consumer - samweb project-summary failed" 
    fi
  fi

  local TEMP

  # convert the project name to the full project url
  if ! TEMP=`samweb find-project $SAM_PROJECT` ; then
    mgf_date "ERROR mgf_start_sam_consumer - samweb find-project failed" 
    return 2
  fi
  export SAM_PROJECT_URL=$TEMP

  [ $AV ] && echo SAM_PROJECT_URL=$SAM_PROJECT_URL

  local N=${SAM_FILE_LIMIT:-0}
  if ! TEMP=`samweb start-process --appname=null --appversion=0 --max-files=$N $SAM_PROJECT` ; then
    mgf_date "ERROR in mgf_start_sam_consumer - samweb start-process failed" 
    return 3
  fi
  export SAM_CONSUMER_ID=$TEMP

  [ $AV ] && echo SAM_CONSUMER_ID=$SAM_CONSUMER_ID

  return 0

}
export -f mgf_sam_start_consumer

# mgf_sam_getnextfile
#
# Get the next file for a consumer.
# This function is only used if mu2e executable is not used to read files.
# The job should have been submitted with sam settings
# You need to setup ifdh and sam_web_client
# Requires SAM_CONSUMER_ID to be set (mgf_start_sam_consumer will do that)
#
# returns environmental SAM_FILE_URL (probably a gridftp url suitable for ifdh)
# and SAM_FILE which is the basename.  These are empty if there are no more files.
#
# -v verbose
#
mgf_sam_getnextfile() {

  local AV=""
  local OPT OPTIND
  while getopts "v" OPT; do
    case $OPT in
      v)
        AV=true;
        ;;
     esac
  done

  export SAM_FILE_URL=""
  export SAM_FILE=""

  if [ "`which samweb`" == "" ]; then
    mgf_date "ERROR mgf_sam_getnextfile - samweb_client not setup"
    return 1
  fi

  if [ ! $SAM_PROJECT_URL ]; then
    mgf_date "ERRROR mgf_start_sam_consumer - SAM_PROJECT_URL must be set to use this function"
    return 1
  fi
  if [ ! $SAM_CONSUMER_ID ]; then
    mgf_date "ERRROR mgf_start_sam_consumer - SAM_PROJECT must be set to use this function"
    return 1
  fi

  local TEMP

  # convert the project name to the full project url
  if ! export TEMP=`samweb get-next-file $SAM_PROJECT_URL $SAM_CONSUMER_ID` ; then
    mgf_date "ERROR mgf_start_sam_consumer - samweb get-next-file failed" 
    return 1
  fi
  export SAM_FILE_URL=$TEMP

  [ $AV ] && echo SAM_FILE_URL=$SAM_FILE_URL

  [ "$SAM_FILE_URL" != "" ] && export SAM_FILE=`basename $SAM_FILE_URL`

  [ $AV ] && echo SAM_FILE=$SAM_FILE

  return 0
}
export -f mgf_sam_getnextfile


#  mgf_sam_releasefile
#
# If getnextfile was called to get a file, then the file should be 
# released with this method.  This function is not used if 
# an art executable is used with sam switches.
# Requires  SAM_PROJECT_URL  SAM_CONSUMER_ID  SAM_FILE_URL
# Ideally, you set SAM_FILE_STATUS=ok   (or notOk) according
# to whether the procesing was successfull.  You can also pass
# this as the first argument.
# You need to setup sam_web_client
#
mgf_sam_releasefile() {

  if [ "`which samweb`" == "" ]; then
    mgf_date "ERROR mgf_sam_releasefile - samweb_client not setup"
    return 1
  fi

  if [[ "$SAM_PROJECT_URL" == "" ||  "$SAM_CONSUMER_ID" == "" || \
                              "$SAM_FILE_URL" == "" ]]; then
    mgf_date "ERROR in mgf_releasefile - SAM_PROJECT, SAM_CONSUMER_ID and SAM_FILE_URL must beset"
    return 1
  fi

  local STATUS=${1:-$SAM_FILE_STATUS}
  STATUS=${STATUS:-OK}
  if [[ "$STATUS" != "ok" && "$STATUS" != "notOk"  ]]; then
    mgf_date "ERROR - mgf_sam_releasefile status=$STATUS, it can only be ok or notOk"
    return 2
  fi

  samweb release-file --status=$STATUS  $SAM_PROJECT_URL $SAM_CONSUMER_ID $SAM_FILE_URL
  local RC=$?

  return $RC
}
export -f mgf_sam_releasefile


# mgf_sam_stop_consumer
#
# Stops the consumer for the sam project.
# Requires SAM_PROJECT and SAM_CONSUMER_ID
# to be set.
#
# -v verbose
#
mgf_sam_stop_consumer() {

  local AV=""
  local OPT OPTIND
  while getopts "v" OPT; do
    case $OPT in
      v)
        AV=true;
        ;;
     esac
  done

  if [ "`which samweb`" == "" ]; then
    mgf_date "ERROR mgf_sam_releasefile - samweb_client not setup"
    return 1
  fi

  if [[ "$SAM_PROJECT" == "" || "$SAM_CONSUMER_ID" == "" ]]; then
    mgf_date "ERRROR mgf_stop_sam_consumer - SAM_PROJECT andSAM_CONSUMER_ID  must be set"
    return 1
  fi

  if ! samweb stop-process $SAM_PROJECT_URL $SAM_CONSUMER_ID ; then
      mgf_date "ERROR mgf_stop_sam_consumer - samweb stop-process failed" 
  fi

  # print a summary of the project
  if [ $AV ]; then
     if ! samweb project-summary $SAM_PROJECT ; then
       mgf_date "ERROR mgf_stop_sam_consumer - samweb project-summary failed" 
    fi
  fi

}
export -f mgf_sam_stop_consumer


# mgf_start project
#
# Start a SAM project. Not used in typical grid jobs.
# Requires SAM_DD to be set to a dataset definition
# to be set.  If SAM_PROJECT is set, it is used.
# Sets SAM_PROJECT and SAM_PROJECT_URL
#
# -v verbose
#
mgf_start_project() {

  local AV=""
  local OPT OPTIND
  while getopts "v" OPT; do
    case $OPT in
      v)
        AV=true;
        ;;
     esac
  done

  if [ "`which samweb`" == "" ]; then
    mgf_date "ERROR mgf_sam_releasefile - samweb_client not setup"
    return 1
  fi

  if [ "$SAM_DD" == "" ]; then
    mgf_date "ERRROR mgf_stop_sam_consumer - SAM_DD must be set"
    return 1
  fi

  local PROJECT="${SAM_DD}_`date +%s`"
  export SAM_PROJECT=${SAM_PROJECT:-$PROJECT}

  local TEMP
  if ! TEMP=`samweb start-project --defname=$SAM_DD $SAM_PROJECT` ; then
    mgf_date "ERROR - mgf_start_project failed to start project"
    return 1
  fi
  export SAM_PROJECT_URL=$TEMP

  [ $AV ] && echo SAM_PROJECT=$SAM_PROJECT
  [ $AV ] && echo SAM_PROJECT_URL=$SAM_PROJECT_URL

  return 0

}
export -f mgf_start_project


# mgf_stop_project
#
# Stop a SAM project. Not used in typical grid jobs.
# Requires SAM_PROJECT to be set.
#
# -v verbose
#
mgf_stop_project() {

  local AV=""
  local OPT OPTIND
  while getopts "v" OPT; do
    case $OPT in
      v)
        AV=true;
        ;;
     esac
  done

  if [ "`which samweb`" == "" ]; then
    mgf_date "ERROR mgf_sam_releasefile - samweb_client not setup"
    return 1
  fi

  if [ "$SAM_PROJECT" == "" ]; then
    mgf_date "ERRROR mgf_stop_sam_consumer - SAM_PROJECT must be set"
    return 1
  fi

  if ! samweb project-summary $SAM_PROJECT ; then
    mgf_date "ERROR - mgf_stop_project failed to stop project"
    return 1
  fi

  # print a summary of the project
  if [ $AV ]; then
     if ! samweb project-summary $SAM_PROJECT ; then
       mgf_date "ERROR mgf_stop_project - samweb project-summary failed" 
    fi
  fi

  return 0

}
export -f mgf_stop_project



# mgf_ifdh_with_backoff
#
# Execute an ifdh command with retries and backoff
# Default command is "ifdh cp $1 $2", can be redefined with
# MGF_IFDH_COMMAND.  The retries have the following
# sleep pattern "600 1800 3600 3600" in seconds, which can be changed
# with MGF_IFDH_SLEEP_PLAN
#
#
mgf_ifdh_with_backoff() {

  local IFDH_COMMAND=${MGF_IFDH_COMMAND:-"ifdh cp $1 $2"}
  local SLEEP_PLAN=${MGF_IFDH_SLEEP_PLAN:-"600 1800 3600 3600"}
  # add a zero, just makes the loop simplier
  SLEEP_PLAN="0 $SLEEP_PLAN"
  local RC=0

  local N=0
  for SLEEP in $SLEEP_PLAN
  do
    sleep $SLEEP

    $IFDH_COMMAND
    RC=$?

    N=$(($N+1))
    if [ $RC -eq 0 ]; then
      return 0
    else
      mgf_date "ERROR - mgf_ifdh_with_backoff failed on try $N with return code $RC"
    fi
  done

  return $RC

}
export -f mgf_ifdh_with_backoff
