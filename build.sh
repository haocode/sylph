#!/bin/bash

#JAVA10_HOME
#echo JAVA10_HOME=$JAVA10_HOME
#export JAVA_HOME=$JAVA10_HOME
#export PATH=$JAVA10_HOME/bin:$PATH
java -version

#./gradlew -v


/home/admin/sylph/sylph-dist/build/bin/launcher stop

./gradlew assemble "$@"

#./gradlew clean checkstyle assemble test "$@"

/home/admin/sylph/sylph-dist/build/bin/launcher start