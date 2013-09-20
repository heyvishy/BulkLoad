#!/bin/sh
# This is a comment
echo "Executing BulkLoad:"
java -cp .:lib/log4j-1.2.16.jar com.wmg.BulkLoad
echo "done."