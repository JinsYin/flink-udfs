#!/bin/bash

###############################################################################
#
# Update the version of the Maven project
#
# Usage:
# - ./update-version.sh                # automatically determine new version
# - ./update-version.sh 0.1.0-SNAPSHOT # set a concrete new version
#
###############################################################################

# set a concrete new version (optional)
newVersion=$1

# get next iteration version
getNextVersion() {
  currVersion=$1;
  # extract major version without the SNAPSHOT
  majorVersion=$(echo $currVersion | cut -d'-' -f1)
  # extract patch version
  revision=$(echo $majorVersion | awk -F. '{print $3}')
  if [[ $currVersion == *"-SNAPSHOT" ]]; then
    # if the current version is end with the -SNAPSHOT, then remove the -SNAPSHOT
    nextVersion=$(echo $majorVersion | sed 's/-SNAPSHOT//')
  else
    # if the current version is not end with the -SNAPSHOT, then add the -SNAPSHOT
    newRevision=$(($revision + 1))
    nextVersion=$(echo $majorVersion | awk -F. -v OFS=. '{$3=""; print $0}')$newRevision-SNAPSHOT
  fi
  echo $nextVersion;
}

# current project version
currVersion=`mvn help:evaluate -Dexpression=project.version -q -DforceStdout`

# next iteration version
nextVersion=`if [ $newVersion ]; then echo $newVersion; else getNextVersion $currVersion; fi`

echo "Current project version : $currVersion"
echo "Next iteration version  : $nextVersion"

# update the version of the POM
mvn versions:set -DnewVersion=$nextVersion -DgenerateBackupPoms=false

# update the documentation version
os=$(uname -s) # os type
if [ "$os" = "Darwin" ]; then
  sed -i '' "s|<version>$currVersion</version>|<version>$nextVersion</version>|g" README.md
else
  sed -i "s|<version>$currVersion</version>|<version>$nextVersion</version>|g" README.md
fi
