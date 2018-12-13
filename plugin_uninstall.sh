#!/bin/bash
echo "#1 plugin disable humpback-logdriver."
docker plugin disable humpback-logdriver
echo "#2 plugin remove humpback-logdriver."
docker plugin rm -f humpback-logdriver
echo "#3 remove rootfs."
rm -rf rootfs
