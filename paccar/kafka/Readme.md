Mock Kafka with docker
=======================

This project allows you to create kafka topics for testing with a docker image.

Docker Commands
================

To create the docker resouce: `docker compose up -d `
to tear down: `docker compose down `

Producing Topics
================

`python producer_generic.py -h`


If no number is passed as an argument, the producer will continue to produce topics indefinitley.

Consuming Topics
===============

`python consume_generic.py -h`
