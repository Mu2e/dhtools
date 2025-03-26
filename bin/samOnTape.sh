#! /bin/bash
#
# Ray Culbertson
#

################################
# usage function
################################
usage() {
echo "
   samOnTape DATASET
      -h print help
  
     Summarize how many files of the dataset have a 
     location on tape.  Generally, once a file has a tape location 
     it is backed up and safe to delete the orginal.

     DATASET is the name of a dataset to examine
"
}


################################
### main
################################

while getopts h OPT; do
    case $OPT in
        h)
            usage
            exit 0
            ;;
        *)
            echo unknown option, exiting
	    usage
            exit 1
            ;;
     esac
done

RC=0
if [ -z "$MU2E" ]; then
    echo "please setup mu2e"
    RC=1
fi
if ! command -v samweb >& /dev/null; then
    echo "please setup sam-web-client"
    RC=1
fi
[ $RC -ne 0 ] && exit 1

DS="$1"
if [ "$DS" == "" ]; then
  echo "ERROR - no dataset provided"
  usage
  exit 1
fi

NT=`samweb list-files --summary "dh.dataset=$DS" | grep File | awk '{print $3}'`
NP=`samweb list-files --summary "dh.dataset=$DS and tape_label %" | grep File | awk '{print $3}'`

NN=$(($NT-$NP))
printf "%4d Total   %4d on tape  %4d not on tape\n" $NT $NP $NN

exit 0
