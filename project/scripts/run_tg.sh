#!/bin/bash

echo "------------Starting Tinkergraph---------------"

for name in `docker ps --format "{{.Names}}" `;
  do
     echo "Starting Tinkergraph in "${name}
     docker exec -w /apache-tinkerpop-gremlin-server-3.4.0 -d $name bash -c "stdbuf -oL -eL bin/gremlin-server.sh ../project/gremlin-server.yaml 2> /tmp/log/${name}_tg.e 1> /tmp/log/${name}_tg.o" &
done;

