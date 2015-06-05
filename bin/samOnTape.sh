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
  
     Summarize how many files have a location on tape.
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

export SAM_EXPERIMENT=mu2e

# will need sam_web_client, setup if not already there
[ -z "$SETUP_SAM_WEB_CLIENT" ] && setup sam_web_client 
if [ -z "$SETUP_SAM_WEB_CLIENT" ]; then
  echo "ERROR - could not setup sam_web_client"
  exit 1
fi

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
