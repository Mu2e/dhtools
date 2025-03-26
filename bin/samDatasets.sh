#! /bin/bash
#
# Ray Culbertson
#

usage() {
echo "
   samDatasets

     Print a list of mu2e datasets in SAM
"
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


# write the sam file names to TMPF

TMPF=/tmp/${USER}_samdsf_$$
TMP=/tmp/${USER}_samds_$$
rm -f $TMPF $TMP

samweb -e mu2e list-parameters dh.dataset | grep mu2e    >> $TMPF
samweb -e mu2e list-parameters dh.dataset | grep -v mu2e >> $TMPF

echo " files     MB      events     dataset"

while read DS
do

  rm -f $TMP
  samweb list-files --summary "dh.dataset=$DS" > $TMP
  NN=`cat $TMP | grep File | awk '{print $3}'`
  # in GB
  SZ=`cat $TMP | grep Total | awk '{printf "%d",$3/1000000.0}'`
  EC=`cat $TMP | grep Event | awk '{printf "%d",$3}'`

  #echo $DS NN $NN SZ $SZ  EC $EC
  if [ $NN -gt 0 ]; then
    printf "%6d %7d %10d   %s\n" $NN  $SZ $EC $DS
  fi

done < $TMPF

rm -f $TMP $TMPF


exit 0
