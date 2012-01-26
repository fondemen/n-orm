#! /bin/sh
#
# Deploy all HBase dependencies which are not available via the official
#	 maven repository at http://repo1.maven.org.
#
#
# This is for HBase 0.20.0
#
# Modified for HBase 0.20.0 from the original located at
# http://www.fiveclouds.com/2009/04/13/deploying-hbase-to-your-local-maven-repo/
#
# The maven repository to deploy to.
#

REPOSITORY_URL=file:///$HOME/.m2/repository

if [ -z $HBASE_HOME ]; then
	echo "Error: HBASE_HOME is not set." 2>&1
	exit 1
fi

HBASE_LIBDIR=$HBASE_HOME/lib

# HBase
#
mvn deploy:deploy-file -Dfile=$HBASE_HOME/hbase-0.20.6.jar \
	-DpomFile=pom.xml -Durl=$REPOSITORY_URL

#Hadoop
mvn deploy:deploy-file -DgroupId=org.apache -DartifactId=hadoop \
	-Dversion=0.20.2 -Dpackaging=jar -Durl=$REPOSITORY_URL \
	-Dfile=$HBASE_LIBDIR/hadoop-0.20.2-core.jar

#thrift
mvn deploy:deploy-file -DgroupId=com.facebook -DartifactId=thrift \
	-Dversion=r771587 -Dpackaging=jar -Durl=$REPOSITORY_URL \
	-Dfile=$HBASE_LIBDIR/libthrift-r771587.jar

#apache commons cli
mvn deploy:deploy-file -DgroupId=commons-cli -DartifactId=commons-cli \
	-Dversion=2.0 -Dpackaging=jar -Durl=$REPOSITORY_URL \
	-Dfile=$HBASE_LIBDIR/commons-cli-2.0-SNAPSHOT.jar

#zookeeper
mvn deploy:deploy-file -DgroupId=org.apache.hadoop -DartifactId=zookeeper \
	-Dversion=3.2.2 -Dpackaging=jar -Durl=$REPOSITORY_URL \
	-Dfile=$HBASE_LIBDIR/zookeeper-3.2.2.jar

#jetty
mvn deploy:deploy-file -DgroupId=org.mortbay.jetty -DartifactId=jetty \
	-Dversion=6.1.14 -Dpackaging=jar -Durl=$REPOSITORY_URL \
	-Dfile=$HBASE_LIBDIR/jetty-6.1.14.jar

# EOF

