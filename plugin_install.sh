#!/bin/bash
echo "#1 image build."
docker build -t humpback-logdriver .
ID=$(docker create humpback-logdriver true)
echo "#2 mkdir rootfs."
rm -rf rootfs
mkdir rootfs
echo "#3 export to rootfs."
docker export "$ID" | tar -x -C rootfs/
echo "#4 remove temp container."
docker rm -vf "$ID"
echo "#5 create plugin."
docker plugin create humpback-logdriver .
echo "#6 enable plugin."
docker plugin enable humpback-logdriver
echo "#7 successed."
