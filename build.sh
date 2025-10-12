#!/bin/bash

export JAVA_HOME=/opt/jdk-25
export M2_HOME=/opt/maven

$M2_HOME/bin/mvn clean package

exit 0
