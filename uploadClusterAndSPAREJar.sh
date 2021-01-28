#!/bin/bash

./gradlew clusteringSpareJar

scp ./build/libs/*-clustering-spare.jar fnaldini@137.204.72.84:/home/fnaldini/first_impact