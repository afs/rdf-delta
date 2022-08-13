# Build and Release Processes

## Build

```
mvn clean install
```

## Release

Edit `release-setup` and commit to main.
All files must be commited

Setup: this sets environment variables:

```
source release-setup
```

Dry run:
```
   mvn $MVN_ARGS release:clean release:prepare 
   mvn $MVN_ARGS release:clean
```

Release:
```
mvn $MVN_ARGS release:prepare 
mvn $MVN_ARGS release:perform
```
If it goes wrong:
```
mvn $MVN_ARGS release:rollback
mvn $MVN_ARGS release:clean
```
or
```
mvn  $MVN_ARGS versions:set -DnewVersion=Old version
find . -name \*.versionsBackup | xargs rm
```
Delete tags.

