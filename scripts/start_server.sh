#!/usr/bin/env bash
docker run --rm -p 127.0.0.1:5432:5432 --name postgresql -e POSTGRES_USER=postgres -e POSTGRES_DB=travis_ci_test postgres:9.4
