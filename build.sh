#!/bin/bash
function check {
	if [ $? -ne 0 ]; then
		echo "ERROR"
		exit $?
	fi
}

function install {
	cd ../$1
	if [ "$2" = "test" ]; then
		mvn clean test
	elif [ "$2" = "final" ]; then
		mvn clean install assembly:single
	else
		mvn clean install
	fi
	check
}

function deploy {
	cd ../$1
	if [ "$2" = "jar" ]; then
		mvn clean deploy site-deploy -P release
	elif [ "$2" = "final" ]; then
		mvn clean deploy assembly:single site-deploy -P release
	elif [ "$2" = "test" ]; then
		mvn clean test
	else
		mvn clean deploy
	fi
	check
}

if [ "$1" = "install" ]; then
	echo "Installing project artifact into local maven repository"
elif [ "$1" = "deploy" ]; then
	echo "Deploying project artifact into maven repository"
else
	echo "Usage `basename $0` [install|deploy]"
	echo "	install: compile and install into local maven repository"
	echo "	deploy: compile and install into OSS repository"
	exit 1
fi

cd `dirname $0`/parent


$1 parent
$1 parent-aspect
$1 storage jar
$1 hbase-test-deps
$1 hbase final

$1 sample test

echo "$1 completed with no error"

cd ..
echo "The following assemblies were genrated:"
find . -type f -name *jar-with-dependencies.jar

exit 0
