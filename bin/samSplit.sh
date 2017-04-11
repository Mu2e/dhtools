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

# must be ready to use sam_web_client
# which needs a grid cert
if ! grid-proxy-info >& /dev/null ; then
  echo "ERROR - grid certificate not found.  Please:
  kinit
  getcert
  export X509_USER_CERT=/tmp/x509up_u\`id -u\`"
  exit 2
fi

export SAM_EXPERIMENT=mu2e

# will need sam_web_client, setup if not already there
[ -z "$SETUP_SAM_WEB_CLIENT" ] && setup sam_web_client 

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
