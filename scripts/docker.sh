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

        CURL=$(curl -s -X GET 'http://127.0.0.1:8086/query')

        while [ "$?" -ne 0 ]; do
            CURL=$(curl -s -X GET 'http://127.0.0.1:8086/query')
            echo -n '.'
            sleep 1
        done
        ;;

    "stop")
        docker stop "$NAME"
        docker rm "$NAME"
        ;;
    *)
        echo "Usage: $0 [start|stop]"
esac
