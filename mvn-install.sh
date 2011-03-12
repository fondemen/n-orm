#!/bin/bash

cd parent
mvn clean install
cd ../parent-aspect
mvn clean install
cd ../storage
mvn clean install
cd ../hbase-test-deps
mvn clean install
cd ../hbase
mvn clean install

cd ../sample
mvn test
