#!/bin/bash

#JAVA10_HOME
#echo JAVA10_HOME=$JAVA10_HOME
#export JAVA_HOME=$JAVA10_HOME
#export PATH=$JAVA10_HOME/bin:$PATH
java -version

#./gradlew -v

git pull

/home/admin/sylph/sylph-dist/build/bin/launcher stop

./gradlew assemble "$@"

/home/admin/sylph/sylph-dist/build/bin/launcher start


#./gradlew clean checkstyle licenseMain licenseTest assemble test "$@"
