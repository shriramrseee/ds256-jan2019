#!/bin/bash

echo "----------Killing Process-----------"

for name in `docker ps --format "{{.Names}}" `;
  do
         echo "Killing Process in "${name}
         docker exec -w / -d $name bash -c "kill -- -1" &
done;