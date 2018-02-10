#! /bin/bash
set -o errexit
set -o nounset

commit_msg=$1

pushd ..
	git pull
	git commit -m "$commit_msg"
	git push
popd