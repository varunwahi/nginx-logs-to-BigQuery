#! /bin/sh

python3 export.py && /usr/sbin/crond -f -l 8
