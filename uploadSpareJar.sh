#!/bin/bash

./gradlew spareJar

scp ./build/libs/*-spare.jar fnaldini@137.204.72.84:/home/fnaldini/first_impact
