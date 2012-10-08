# constants
DATABUS_MIRROR_STREAM_VALIDATION_CLASS="com.inmobi.databus.utils.MirrorStreamDataConsistencyValidation"
DATABUS_ORDERLY_CREATION_FILES_CLASS="com.inmobi.databus.utils.OrderlyCreationOfDirs"
DATABUS_MERGE_STREAM_VALIDATION_CLASS="com.inmobi.databus.utils.MergeStreamDataConsistency"
DATABUS_LOCAL_STREAM_VALIDATION_CLASS="com.inmobi.databus.utils.LocalStreamDataConsistency"
#functions
info() {
  local msg=$1

  echo "Info: $msg" >&2
}

warn() {
  local msg=$1

  echo "Warning: $msg" >&2
}

error() {
  local msg=$1
  local exit_code=$2

  echo "Error: $msg" >&2

  if [ -n "$exit_code" ] ; then
    exit $exit_code
  fi
}

display_help() {
  cat <<EOF
USAGE: $0 mirrorstreamdataconsistency <mergedstreamroot-dir> <mirrorstreamroot-dir (comma separated list)> [<streamname (comma separated list)>]
       $0 orderlycreated <root-dirs (comma separated list)> [<basedir (comma separated list)>] [<streamname (comma separated list)>]
       $0 mergestreamdataconsistency <local stream root-dirs (comma separated list)> <merge stream root-dir> [<streamNames (comma separated list)>]
       $0 localstreamdataconsistency <root-dirs (comma separated list)>
EOF
}

run_databus() {
  local DATABUS_UTILITY_CLASS

  if [ "$#" -gt 0 ]; then
    DATABUS_UTILITY_CLASS=$1
    shift
  else
    error "Must specify databus utility class" 1
  fi

  set -x
  exec $JAVA_HOME/bin/java $JAVA_OPTS -cp "$DATABUS_CLASSPATH" \
      "$DATABUS_UTILITY_CLASS" $*

}

################################
# main
################################

# set default params
DATABUS_CLASSPATH=""
DATABUS_JAVA_LIBRARY_PATH=""
JAVA_OPTS="-Xmx128M"


mode=$1
shift

case "$mode" in
  help)
    display_help
    exit 0
    ;;
  mirrorstreamdataconsistency)
    opt_mirror=1
    ;;
  orderlycreated)
    opt_order=1;
    ;;
  mergestreamdataconsistency)
    opt_local_merge=1;
    ;;
  localstreamdataconsistency)
    opt_local=1;
    ;;
  *)
    error "Unknown or unspecified command '$mode'"
    echo
    display_help
    exit 1
    ;;
esac

while [ -n "$*" ] ; do
  arg=$1
  shift

  case "$arg" in
    -D*)
      JAVA_OPTS="${JAVA_OPTS} $arg"
      ;;
    *)
      args="$args $arg"
      ;;
  esac
done


#find java
if [ -z "${JAVA_HOME}" ] ; then
  echo "Warning: JAVA_HOME not set!"
    JAVA_DEFAULT=`type -p java`
    [ -n "$JAVA_DEFAULT" ] || error "Unable to find java executable. Is it in your PATH?" 1
    JAVA_HOME=$(cd $(dirname $JAVA_DEFAULT)/..; pwd)
fi

[ -n "${JAVA_HOME}" ] || error "Unable to find a suitable JAVA_HOME" 1


# figure out where the client distribution is
if [ -z "${DATABUS_HOME}" ] ; then
  DATABUS_HOME=$(cd $(dirname $0)/..; pwd)
fi

# Append to the classpath
if [ -n "${DATABUS_CLASSPATH}" ] ; then
  DATABUS_CLASSPATH="${DATABUS_HOME}/lib/*:$DEV_CLASSPATH:$DATABUS_CLASSPATH"
else
  DATABUS_CLASSPATH="${DATABUS_HOME}/lib/*:$DEV_CLASSPATH"
fi

# finally, invoke the appropriate command
if [ -n "$opt_order" ] ; then
  run_databus $DATABUS_ORDERLY_CREATION_FILES_CLASS $args 
elif [ -n "$opt_mirror" ] ; then
  run_databus $DATABUS_MIRROR_STREAM_VALIDATION_CLASS $args $args1 $args2
elif [ -n "$opt_local_merge" ] ; then 
  run_databus $DATABUS_MERGE_STREAM_VALIDATION_CLASS $args
elif [ -n "$opt_local" ] ; then
  run_databus $DATABUS_LOCAL_STREAM_VALIDATION_CLASS $args
#  echo $args
else
  error "This message should never appear" 1
fi

exit 0

