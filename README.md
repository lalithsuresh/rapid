[![Build Status](https://travis-ci.org/lalithsuresh/rapid.svg?branch=master)](https://travis-ci.org/lalithsuresh/rapid) [![Coverage Status](https://coveralls.io/repos/github/lalithsuresh/rapid/badge.svg?branch=master)](https://coveralls.io/github/lalithsuresh/rapid?branch=master)


Rapid is a distributed membership service. It allows distributed processes to easily form clusters 
and notifies processes when the membership changes.


Building Rapid
==============

To build Rapid, enter the following command in the top-level directory:

```
$: mvn package
```


Running Rapid
=============

To launch a simple Rapid-based agent, run the following commands in your shell from the top-level directory:

```
  $: java -jar rapid-examples/target/standalone-agent.jar --listenAddress 127.0.0.1:1234 --seedAddress 127.0.0.1:1234
```

From two other terminals, try adding a few more nodes on different listening addresses, but using the
same seed address of "127.0.0.1:1234". For example:

```
  $: java -jar rapid-examples/target/standalone-agent.jar --listenAddress 127.0.0.1:1235 --seedAddress 127.0.0.1:1234
  $: java -jar rapid-examples/target/standalone-agent.jar --listenAddress 127.0.0.1:1236 --seedAddress 127.0.0.1:1234
```

Or use the following script to start multiple agents in the background that bootstrap via
node 127.0.0.1:1234.

```
  #! /bin/bash
  for each in `seq 1235 1245`;
  do
        java -jar rapid-examples/target/standalone-agent.jar --listenAddress 127.0.0.1:$each --seedAddress 127.0.0.1:1234  &> /tmp/rapid.$each &
  done
```
