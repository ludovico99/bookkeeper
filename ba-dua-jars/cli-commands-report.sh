#!/bin/sh


#BADUACLI="/home/runner/work/bookkeeper/bookkeeper/ba-dua-jars/ba-dua-cli-0.6.0-all.jar"
#BADUASER="/home/runner/work/bookkeeper/bookkeeper/bookkeeper-server/target/badua.ser"
#CLASSES="/home/runner/work/bookkeeper/bookkeeper/bookkeeper-server/target/classes"
#BADUAXML="/home/runner/work/bookkeeper/bookkeeper/bookkeeper-server/target/badua.xml"

BADUACLI="../ba-dua-jars/ba-dua-cli-0.6.0-all.jar"
BADUASER="./target/badua.ser"
CLASSES="./target/classes"
BADUAXML="./target/badua.xml"

java -jar ${BADUACLI} report    \
        -input ${BADUASER}      \
        -classes ${CLASSES}     \
        -show-classes           \
        -show-methods           \
        -xml ${BADUAXML}