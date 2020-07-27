#!/bin/bash
SCRIPTPATH="$( cd "$(dirname "$0")" ; pwd -P )"
echo "Running API Tests at $SCRIPTPATH"

$SCRIPTPATH/run_test.sh 'iqe tests plugin cost_management -k test_api'
