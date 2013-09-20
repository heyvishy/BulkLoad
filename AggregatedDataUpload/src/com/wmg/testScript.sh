#!/bin/sh
# This is a comment
echo "Executing BulkLoad:"
 java -cp .:lib/log4j-1.2.16.jar -Dlog4j.configuration=log4j.properties com.wmg.BulkLoad
echo "done."