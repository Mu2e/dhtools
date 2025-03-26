#! /bin/bash
#
# Ray Culbertson
#

################################
# usage function
################################
usage() {
echo "
   samOnDisk DATASET
      -h print help
  
     This script will start randomly picking files of the 
     dataset to check whether they are currently on disk in
     dCache or only on tape.  If more than a few percent are
     only on tape, then the dataset should be prestaged.
     Once the situation is clear, you can control-c, or
     the script will stop after 1000 checks.

     DATASET is the name of a dataset to examine, or the name
     of a dataset definition
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

TMP=`mktemp`
echo "Listing files..."
samweb list-definition-files $DS > $TMP
NT=`cat $TMP | wc -l`
echo "Found $NT files"

NN=1
ND=0
while [[ $NN -le 1000 && $NN -le $NT ]]; do
  if [ $NT -lt 1000 ]; then
    # take all the files in order
    SF=`cat $TMP | awk '{if(NR=='$NN') print $0}'`
  else
    # take random
    SF=`cat $TMP | awk 'BEGIN{srand(); i=int('$NT'*rand())+1}{if(NR==i) print $0}'`
  fi
  SLFS=`samweb locate-file $SF | grep enstore`
  #echo SLFS=$SLFS
  PNFSDIR=` echo $SLFS | awk -F: '{print $2}' | sed -e 's/([^()]*)//g' `
  #echo PNFS=$PNFS
  #echo SF=$SF
  ANS=`cat $PNFSDIR/'.(get)('$SF')(locality)'`
  #echo $ANS
  if [[ "$ANS" =~ "ONLINE" ]]; then
    ND=$(($ND+1))
  fi
  RR=`echo $ND $NN | awk '{print $1*100/$2}'`
  printf "%4d/%4d are on disk, %4.1f %%\n" $ND $NN $RR
  NN=$(($NN+1))
done

rm -f $TMP

exit 0
