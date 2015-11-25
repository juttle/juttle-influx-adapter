#!/bin/bash

NAME="influxdb"
VER="1"
CONTAINER="juttle/$NAME:$VER"

CMD="$1"

case "$CMD" in
    "start")
        docker stop "$NAME"
        docker rm "$NAME"

        docker pull "$CONTAINER"
        docker run -d --name "$NAME" -p 8083:8083 -p 8086:8086 "$CONTAINER"
        ;;
    "stop")
        docker stop "$NAME"
        docker rm "$NAME"
        ;;
    *)
        echo "Usage: $0 [start|stop]"
esac
