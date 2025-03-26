#! /bin/bash
#
# Ray Culbertson
#

usage() {
echo "
   samSplit DATASET TAG N

     Take a dataste and split into several smaller 
     subset and print the new dataset definitions.
     DATASET the input dataset
     TAG a string that makes the new dataset definition unique
     N how many pieces to split the dataset into
"
}


if [ $# -ne 3 ]; then
  usage
  exit 1
fi

DS="$1"
TAG="$2"
NSPL=$3

RC=0
if [ -z "$MU2E" ]; then
    echo "please setup mu2e"
    RC=1
fi
if ! command -v samweb >& /dev/null; then
    echo "please setup sam-web-client"
    RC=1
fi
if ! seeToken >& /dev/null; then
    echo "please refresh your token"
    RC=1
fi
[ $RC -ne 0 ] && exit 1

NFILES=`samweb count-files "dh.dataset=$DS"`
SNAPNO=`samweb take-snapshot $DS`

if [ $NFILES -lt $NSPL  ]; then
  echo "$NFILES files is less than requested $NSPL splits"
  return 1
fi


BASESTEP=$(($NFILES/$NSPL))
REMAINDER=$(($NFILES%$NSPL))
NLEFT=$NFILES
N1=1
NDD=0
while [ $NLEFT -gt 0 ];
do
  if [ $REMAINDER -gt 0 ]; then
    STEP=$(($BASESTEP+1))
    REMAINDER=$(($REMAINDER-1))
  else
    STEP=$BASESTEP
  fi
  N2=$(($N1+$STEP-1))

  #echo $NLEFT $STEP $N1 $N2
  NDD=$(($NDD+1))
  NEWDD=${USER}_${TAG}_${NDD}
  samweb create-definition $NEWDD "snapshot_id=$SNAPNO and snapshot_file_number>=$N1 and snapshot_file_number<=$N2"

  echo
  echo $NWEDD
  #samweb list-definition-files $NEWDD
  NNEW=`samweb count-definition-files $NEWDD`
  echo $NEWDD contains $NNEW files


  NLEFT=$(($NLEFT-$STEP))
  N1=$(($N2+1))
done


exit 0
