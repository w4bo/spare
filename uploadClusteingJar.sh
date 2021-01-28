#!/bin/bash

./gradlew snapshotGeneratorJar

scp ./build/libs/*-clustering.jar fnaldini@137.204.72.84:/home/fnaldini/first_impact
