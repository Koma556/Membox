#!/bin/bash

help="Usage: $0 [ FILE ] [ OPTIONS ]...\nReads the [ FILE ] log and digests its output. With no [ OPTIONS ].
this script will print the last timestamp in [ FILE ]\n\n
-p\t\tprints the number of total and failed PUT_OP\n
-u\t\tprints the number of total and failed UPDATE_OP\n
-g\t\tprints the number of total and failed GET_OP\n
-r\t\tprints the number of total and failed REMOVE_OP\n
-c\t\tprints the number of total connections\n
-s\t\tprints the total repository size in KB\n
-o\t\tprints the number of total objects\n
-m\t\tprints the maximum number of concurrent connections\n
--help\t\tthis very utility\n"

path=0

#prints help if no commands given
if [ $# == 0 ]; then
	echo -e $help
	exit 1
fi

#inizialize all to 1 so cut doesn't coplain
three=1
four=1
five=1
six=1
seven=1
eight=1
nine=1
ten=1
elevel=1
thirteen=1
fourteen=1
fifteen=1
sixteen=1
seventeen=1

while [[ $# > 0 ]]
do
key="$1"

case $key in
    -p)
    PUT=true
    put="PUT\tPUTFAIL\t"
    three=3
    four=4
    ;;
    -u)
    UPDATE=true
    update="UPDT\tUPDTFAIL"
    five=5
    six=6
    ;;
    -g)
    GET=true
    get="GET\tGETFAIL\t"
    seven=7
    eight=8
    ;;
    -r)
    REMOVE=true
    remove="RMV\tRMVFAIL\t"
    nine=9
    ten=10
    ;;
    -c)
    CONNECTIONS=true
    connections="CNNCTNS\t"
    thirteen=13
    ;;
    -s)
    SIZE=true
    size="SIZE\t"
    fourteen=14
    ;;
    -o)
    OBJECTS=true
    objects="OBJECTS\t"
    sixteen=16
    ;;
    -m)
    MAXCONN=true
    maxconn="MAXSIZE\tMAXOBJNUM\tMAXCONNEC"
    fifteen=15
    seventeen=17
    ;;
    --help)
    echo -e $help
    exit 1
    ;;
    *)
    path=$1
    ;;
esac
shift # past argument or value
done
if [ $path == 0 ] ; then 
	echo -e "No log file specified. Please refer to --help for proper use of this script." 1>&2 
	exit 1
fi

#check if path actually points to a file
if ! [ -f "${path}" ]; then
	echo -e "${path} is not a file. Please refer to --help for proper use of this script.";
	exit 1
fi

exec {fd}<${path}

####parsing complete####
count=0
max=0
if [[ "$PUT" == "true" || "$GET" == "true" || "$UPDATE" == "true" || "$REMOVE" == "true" || "$CONNECTIONS" == "true" || "$SIZE" == "true" || "$OBJECTS" == "true" || "$MAXCONN" == "true" ]]; then
	echo -e "TIMESTAMP\t"$put$update$get$remove$connections$size$objects$maxconn
	while IFS=' ' read -u ${fd} -r line || [[ -n "$line" ]]; do
		echo -e "$line" | cut --output-delimiter=$'\t' -d ' ' -f 1,${three},${four},${five},${six},${seven},${eight},${nine},${ten},${thirteen},${fourteen},${sixteen},${fifteen},${seventeen} | tr -d '\n'
		if [ "$MAXCONN" == true ] ; then
			count=$(($(echo -e "$line" | cut --output-delimiter=$'\t' -d ' ' -f 13)))			
			if [ "$count" -gt "$max" ]; then
				max=$count
			fi
			echo -e $'\t\t'$max
		fi
	done
else
	tail --lines=1 $path
fi
