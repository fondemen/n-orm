#!/bin/bash

cp target/site/aspectj-report/allclasses-*.html target/site/apidocs/
cp target/site/aspectj-report/package-*.html target/site/apidocs/
cp target/site/aspectj-report/overview-*.html target/site/apidocs/
cp target/site/aspectj-report/constant-values.html target/site/apidocs/
cp target/site/aspectj-report/index-all.html target/site/apidocs/

find target/site/aspectj-report -name package-*.html | awk '{dest = $0; gsub("/aspectj-report/", "/apidocs/", dest); print "cp " $0 " " dest}' | source /dev/stdin
find src/main/java -name *.aj | awk '{dest = $0; gsub("src/main/java", "target/site/aspectj-report", dest); gsub(".aj$", ".html", dest); print dest}'  | awk '{dest = $0; gsub("/aspectj-report/", "/apidocs/", dest); print "cp " $0 " " dest}'  | source /dev/stdin
