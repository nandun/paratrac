make -f Makefile09.data all PROG_DIR=../../modules/tools ORIG_DIR=../../data/data0 YEAR=09 PARALLEL_PARSING=1 ID=`echo |sed 's/.*medline.*n\([0-9]*\)\.xml\.gz/\1/'`

touch %.target

rm -rf data-2009

rm *.target

