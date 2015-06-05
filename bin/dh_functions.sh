#
# bash functions to provide easy reference to scripts
#

jsonMaker() {
  $DHTOOLS_DIR/bin/jsonMaker.py "$@"
}
export -f jsonMaker

samRm() {
  $DHTOOLS_DIR/bin/samRm.sh "$@"
}
export -f samRm

samGet() {
  $DHTOOLS_DIR/bin/samGet.sh "$@"
}
export -f samGet

samPrestage() {
  $DHTOOLS_DIR/bin/samPrestage.sh "$@"
}
export -f samPrestage

samDatasets() {
  $DHTOOLS_DIR/bin/samDatasets.sh "$@"
}
export -f samDatasets

samToPnfs() {
  $DHTOOLS_DIR/bin/samToPnfs.sh "$@"
}
export -f samToPnfs

samNoChildren() {
  $DHTOOLS_DIR/bin/samNoChildren.sh "$@"
}
export -f samNoChildren

samOnTape() {
  $DHTOOLS_DIR/bin/samOnTape.sh "$@"
}
export -f samOnTape
