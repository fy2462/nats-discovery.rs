# nats-discovery.rs

## What's the Nats

[NATS](https://nats.io) is a simple, secure and performant communications system for digital systems, services and devices. NATS is part of the Cloud Native Computing Foundation ([CNCF](https://cncf.io)). NATS has over [40 client language implementations](https://nats.io/download/), and its server can run on-premise, in the cloud, at the edge, and even on a Raspberry Pi. NATS can secure and simplify design and operation of modern distributed systems.

## Why release the Rust discovery service

Rust may be one the most interesting new languages. More and more backend service builded with Rust. on the other hand, the Nats discovery feature is not released now, so I develop the discovery service based on Nats for Rust developer.

## Prepare

Rust: 1.6.0
Nats: 0.20.0

### Launch Nats service

We can launch a Nats server with docker comand, We can follow the [Link](https://docs.nats.io/running-a-nats-service/nats_docker) to config and launch a simple sever.

## Start the discovery service

> cargo run

## Mock a registry node

We can mock a registry node with rust test, so just run the command as follow:

> cargo test

and then, we can see the node registry log in discovery service output.
