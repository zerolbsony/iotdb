#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

echo ---------------------
echo Starting Printing the IoTDB Data Directory Overview
echo ---------------------

source "$(dirname "$0")/../../conf/iotdb-common.sh"
#get_iotdb_include and checkAllVariables is in iotdb-common.sh
VARS=$(get_iotdb_include "$*")
checkAllVariables
export IOTDB_HOME="${IOTDB_HOME}/.."
eval set -- "$VARS"


if [ -n "$JAVA_HOME" ]; then
    for java in "$JAVA_HOME"/bin/amd64/java "$JAVA_HOME"/bin/java; do
        if [ -x "$java" ]; then
            JAVA="$java"
            break
        fi
    done
else
    JAVA=java
fi

JVM_OPTS="-Dsun.jnu.encoding=UTF-8 -Dfile.encoding=UTF-8"

CLASSPATH=""
for f in ${IOTDB_HOME}/lib/*.jar; do
  CLASSPATH=${CLASSPATH}":"$f
done

MAIN_CLASS=org.apache.iotdb.db.tools.IoTDBDataDirViewer

"$JAVA" $JVM_OPTS -cp "$CLASSPATH" "-Dlogback.configurationFile=${IOTDB_HOME}/conf/logback-tool.xml" "$MAIN_CLASS" "$@"
exit $?