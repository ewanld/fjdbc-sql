#! /bin/bash

pushd ..
	mvn test
popd
diff -u ../target/test-classes/StandardSqlTest-ref.txt ../target/test-classes/StandardSqlTest-last.txt --color=auto