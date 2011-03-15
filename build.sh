#!/bin/bash
function check {
	if [ $? -ne 0 ]; then
		echo "ERROR at step " $1
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
	check $1
}

function deploy {
	cd ../$1
	if [ "$2" = "jar" ]; then
		mvn clean deploy -P release
		check $1
		mvn site-deploy -P coverage
	elif [ "$2" = "final" ]; then
		mvn clean deploy assembly:single -P release
		check $1
		mvn site-deploy -P coverage
	elif [ "$2" = "test" ]; then
		mvn clean test 
	else
		mvn clean deploy
	fi
	check $1
}

function report {
	cd ../$1
	if [ "$2" = "jar" ]; then
		mvn clean install
		check $1
		mvn site -P coverage
	elif [ "$2" = "final" ]; then
		mvn clean install
		check $1
		mvn site -P coverage
	elif [ "$2" = "test" ]; then
		mvn clean site
	else
		mvn clean install
	fi
	check $1
}

function coverage {
	cd ../$1
	if [ "$2" = "jar" ]; then
		mvn clean install
		check $1
		mvn cobertura:cobertura -P coverage
	elif [ "$2" = "final" ]; then
		mvn clean install
		check $1
		mvn cobertura:cobertura -P coverage
	elif [ "$2" = "test" ]; then
		mvn clean cobertura:cobertura
	else
		mvn clean install
	fi
	check $1
}

function hudson {
	coverage $1
}


if [ "$1" = "install" ]; then
	echo "Installing project artifact into local maven repository"
elif [ "$1" = "deploy" ]; then
	echo "Deploying project artifact into maven repository"
elif [ "$1" = "report" ]; then
	echo "Installing project artifact into local maven repository and reporting to site"
elif [ "$1" = "coverage" ]; then
	echo "Generating coverage reports"
elif [ "$1" = "hudson" ]; then
	echo "Running hudson"
else
	echo "Usage `basename $0` [install|deploy]"
	echo "	install: compile and install into local maven repository"
	echo "	report: compile and install into local maven repository an generate reports"
	echo "	coverage: generate coverage reports"
	echo "	hudson: hudson goal"
	echo "	deploy: compile and install into OSS repository"
	exit 1
fi

LOC=`dirname $0`
cd $LOC/parent

$1 parent
$1 parent-aspect
$1 storage jar
$1 hbase-test-deps
$1 hbase final

$1 sample test

cd ..

echo "$1 completed with no error"

if [ "$1" = "install" ]; then
	echo "The following assemblies were generated:"
	find $LOC -type f -name *jar-with-dependencies.jar
elif [ "$1" = "deploy" ]; then
	echo "The following assemblies were generated:"
	find $LOC -type f -name *jar-with-dependencies.jar
elif [ "$1" = "report" ]; then
	echo "The following reports were generated:"
	find $LOC -type d -name site
elif [ "$1" = "coverage" ]; then
	echo "The following coverage reports were generated:"
	find $LOC/coverage -type f -name coverage.xml
elif [ "$1" = "hudson" ]; then
	if [ -d coverage  ]; then
		rm -rf coverage
	fi
	mkdir coverage
	for f in `find . -name coverage.xml`; do
		fdir=`dirname $f`;
		mkdir -p coverage/$fdir
		cp $f coverage/$fdir
	done
	echo "The following coverage reports were generated:"
	find $LOC/coverage -type f -name coverage.xml
	$0 install
fi

exit 0
