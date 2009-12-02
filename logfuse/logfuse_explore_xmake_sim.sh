#!/bin/sh

###################################################
## PARAMETERS
RUBY=`which ruby`
PYTHON = `which python`

# -- GXP -- #
RSH=ssh
SITE=tohoku
NODES=000-014

# -- WORKFLOW -- #
WORKFLOW=montage
INPUT=data3
PARALLELISM=150
ETC_PARAM=''

# -- LOGFUSE -- #
LOGFUSE_DIR=~/gmprof
SRC_DIR=/data/3/shibata/wf_set/$WORKFLOW
MNT_DIR=/data/3/shibata/mnt_pnt
LOG_DIR=/data/3/shibata/Experiment/$WORKFLOW/$INPUT
MNT_XMAKE_DIR=$MNT_DIR/solvers/gxp_make

# -- DATA -- #
NML_CMD=$LOGFUSE_DIR/ncmds/normal_cmds_$WORKFLOW.dat
WORKFLOW_DB=$LOG_DIR/workflow.db
DEPENDENCY_DIR=$LOG_DIR/dependency

###################################################
## GXP EXPLORE
gxpc use $RSH $SITE
gxpc explore $SITE[[$NODES]]
#gxpc e -H `hostname`
#gxpc smask

###################################################
## LOGFUSE
mkdir -p $MNT_DIR
mkdir -p $LOG_DIR
gxpc cd $LOGFUSE_DIR
gxpc e ./logfuse $SRC_DIR $MNT_DIR $LOG_DIR

###################################################
## GXP MAKE : should be modified for each workflow
# -- Cleaning up the temp files -- #
cd $SRC_DIR/solvers/gxp_make
make clean INPUT=$INPUT

# -- Starting GXP Make -- #
cd $MNT_XMAKE_DIR
./prepare.sh
echo "__________begin_make"
gxpc make -j $PARALLELISM -k all INPUT=$INPUT $ETC_PARAM \
    -- --state_dir $LOG_DIR \
    --local_exec_cmd 'echo' \
    > $LOG_DIR/xmake.out 2>&1
echo "__________end_make"
###################################################
## MAKE DATABASE & MAKE DEPENDENCY DATA
cd $LOGFUSE_DIR
gxpc e fusermount -u $MNT_DIR
$PYTHON ./log_access.py $LOG_DIR $SITE
$RUBY ./make_db.rb $LOG_DIR $NML_CMD $WORKFLOW_DB
$RUBY ./make_dependency.rb $LOG_DIR $DEPENDENCY_DIR
$PYTHON ./wf_filecost.py $LOG_DIR

############
## GXP QUIT
gxpc quit