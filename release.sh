#!/bin/bash

cd parent
mvn clean deploy
cd ../parent-aspect
mvn clean deploy
cd ../storage
mvn clean deploy site-deploy -P release
cd ../hbase-test-deps
mvn clean deploy
cd ../hbase
mvn clean deploy site-deploy -P release
