#!/usr/bin/env bash

echo "-----------------Pulling Latest Code-------------------"

pushd ds256-jan2019;
ssh-agent bash -c 'ssh-add ../.ssh/id_rsa; git pull origin'

echo "-----------------Deleting Existing Code from Container------------------"

for name in `docker ps --format "{{.Names}}"`;
  do echo "Deleting project code directory from "$name
     docker exec -t $name rm -rf project;
done;

echo "-----------------Copying Latest Code into Container------------------"

for contid in `docker ps --format "{{.ID}}"`;
  do echo "Copying project code directory to "$contid
     docker cp project $contid:/. ;
done;

popd;