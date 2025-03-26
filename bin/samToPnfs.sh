#! /bin/bash
#
# Ray Culbertson
#
# example datasets
# OLD style pnfs organization: 
#       sim.mu2e.example-beam-g4s1.1812a.art
# new style pnfs organization: 
#       sim.mu2e.cd3-beam-g4s4-detflash.v533_v543_v563_v566.art
#

usage() {
echo "

  samToPnfs DATASET|DATASET_DEFINITION|FILE|FILE_OF_FILES

     List the full /pnfs filespec of all files in the request.
     Useful for grid jobs which need a SAM datatset, but use a file 
     list for input instead of SAM.  Output will be sorted on file name.

     DATASET a mu2e dataset
         ex: sim.mu2e.cd3-beam-cs4-detflash.v566.art
     DATASET_DEFINITION a SAM dataset definition
         ex: rlc_small_prestage_1465310962
     FILE  a SAM file name
         ex: sim.mu2e.cd3-beam-cs4-detflash.v566.001002_00000000.art
     FILE_OF_FILES a text file containing SAM files names, one per line

"
}

#
# list files in a standard subdirectory
# DS must be defined as the 5-dot SAM dataset name
#
findMethod() {

  TMP=`mktemp`
  samweb list-files --fileinfo "dh.dataset $DS" > $TMP
  RC=$?
  N=`cat $TMP | wc -l`
  rm -f $TMP

  # data case
  F1=`echo $DS | awk -F. '{print $1}'`
  F2=`echo $DS | awk -F. '{print $2}'`
  F3=`echo $DS | awk -F. '{print $3}'`
  F4=`echo $DS | awk -F. '{print $4}'`
  F5=`echo $DS | awk -F. '{print $5}'`

  # check if this dataset exists in the old location (pre 12/2015)
  ls /pnfs/mu2e/*/$F1/$F2/$F3/$F4/*  > /dev/null 2>&1
  TESTOLD=$?

  if [ $TESTOLD -eq 0 ]; then
    find /pnfs/mu2e/*/$F1/$F2/$F3/$F4/*/*  \
      -name "${F1}.${F2}.${F3}.${F4}.*.${F5}" \
      | sort -t/ -k11,11
  else
     find /pnfs/mu2e/tape/*/$F1/$F2/$F3/$F4/$F5/*/* \
      -name "${F1}.${F2}.${F3}.${F4}.*.${F5}" \
      | sort -t/ -k11,11
  fi
}


#
# operate on a list of SAM file names in a file
# FN must be defined as the file name
#
fileMethod() {

  TMP=`mktemp`
  while read FF
  do
    echo `samweb locate-file $FF` $FF >> $TMP
  done < $FN

  cat $TMP | \
   awk -F"[:( ]" '{print $2"/"$4}'  |  \
   sort -t/ -k11,11

  rm -f $TMP

}


while getopts h OPT; do
    case $OPT in
        h)
            usage
            exit 0
            ;;
        *)
            echo unknown option, exiting
            exit 1
            ;;
     esac
done




SPEC="$1"

if [ -z "$SPEC" ];
then
  echo ERROR- no input dataset or file name
  echo
  usage
  exit 1
fi

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


# if it is a file containing file names
LOCALFILE=`readlink -f $SPEC`
if [ -r "$LOCALFILE" ]; then
  FN=$LOCALFILE
  fileMethod
  exit 0
fi

# if it is a dataset
TEST=`samweb list-parameters dh.dataset | grep -x $SPEC`
if [ -n "$TEST" ]; then
  DS=$SPEC
  findMethod
  exit 0
fi

# if it is a dataset definition
samweb describe-definition $SPEC > /dev/null 2>&1
TEST=$?
if [ $TEST -eq 0 ]; then
  FN=`mktemp`
  samweb list-files "defname: $SPEC" > $FN
  fileMethod
  rm -f $FN
  exit 0
fi

#samweb locate-file $FF | awk -F"[:(]" '{print $2}'`"/$FF
# if it is a SAM file name
samweb locate-file $SPEC > /dev/null 2>&1
TEST=$?
if [ $TEST -eq 0 ]; then
  FN=`mktemp`
  echo $SPEC > $FN
  fileMethod
  rm -f $FN
  exit 0
fi


printf "\nERROR: input did not match any known method\n\n"
usage

exit 1

