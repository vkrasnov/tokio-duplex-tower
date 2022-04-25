# tower-duplex

A [`tower::Service`] that implements a server and a client simultaneously over a
bi-directional channel. As a server it is able to process RPC calls from a remote client,
and as a client it is capable of making RPC calls into a remote server. It is very
convinient in a system that requires asynchronous communication in both directions.

License: MIT OR Apache-2.0
