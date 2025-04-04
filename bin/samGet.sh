#! /bin/bash
#
# Ray Culbertson
#

usage() {
echo "
   samGet [OPTIONS] [-f FILE]  [-s FILEOFNAMES]  [-d DATASET]
      -n N    limit the nmber of files to retrieve
      -o DIR    direct output to directory DIR (deafult=.)
      -h print help

     Find certain files in SAM and copy them to a local directory.
     FILE is the comma-separated list of SAM names of file(s) 
      to be retrieved to the local directory
     FILEOFNAMES is a text file contains the sam names of files
     DATASET is the name of a dataset to retrieve. Since you probably don't 
     want all the files of a dataset, please limit the number with -n
     You need to \"setup mu2e\", kinit and kx509 to run this procedure
     Only for interactive use - do nor run in grid jobs or SAM resources
     will be overloaded.
"
}

export DIR=$PWD
export FILEOPT=""
export DSOPT=""
export FOFOPT=""
export NOPT=1000000
export OUTDIR=$PWD

while getopts d:f:n:o:s:h OPT; do
    case $OPT in
        f)
            export FILEOPT=$OPTARG
            ;;
        d)
            export DSOPT=$OPTARG
            ;;
        s)
            export FOFOPT=$OPTARG
            ;;
        n)
            export NOPT=$OPTARG
            ;;
        o)
            export OUTDIR=$OPTARG
            ;;
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
if ! command -v ifdh >& /dev/null; then
    echo "please setup ifdh"
    RC=1
fi
if ! seeToken >& /dev/null; then
    echo "please refresh your token"
    RC=1
fi
[ $RC -ne 0 ] && exit 1


# write the sam file names to TMPF

TMPF=/tmp/${USER}_samgetf_$$
TMP=/tmp/${USER}_samget_$$
rm -f $TMPF $TMP

# for file names provided with -f
[ "$FILEOPT" != "" ] && echo $FILEOPT | tr "," "\n" >> $TMPF

# for file names provided with -s
[ "$FOFOPT" != "" ] && cat $FOFOPT >> $TMPF

# for file names provided with -d
if [ "$DSOPT" != ""  ]; 
then
  samweb list-files "dh.dataset=$DSOPT" >> $TMPF
fi

# check for empty list
NL=`cat $TMPF | wc -l`
if [ $NL -le 0 ]; then
    echo "ERROR - file selection found no files"
    exit 1
fi

# translate sam file names to urls for ifdh

N=0
while read FN
do
    FNF=`samweb get-file-access-url --location=dcache --schema=http $FN`
    if [ -z "$FNF" ]; then
        FNF=`samweb get-file-access-url --location=enstore --schema=http $FN`
    fi
  if [ -z "$FNF" ]; then
      echo "ERROR - SAM had no location for $FN"
      exit 2
  fi
  echo $FNF " " $OUTDIR/$FN >> $TMP
  N=$(($N+1))
  if [ $N -ge $NOPT ]; then break; fi
done < $TMPF

ifdh cp -f $TMP
rm -f $TMP $TMPF

exit 0
