pRpc
=====

pRpc is a yet another python RPC library.

Key features:

* asyncio-based - no predefined threadpools etc.
  Concurrency is under your control.

* pRpc is symmetric - both peers may publish and call RPC methods
  regardless of who initially established the connection.

* Streaming support - each call may send and/or recieve a message stream
  in addition to arguments enabling efficient implementation of RPC calls
  like 'upload'/'download'.

* Binary serialization using msgpack ensures the low communication overhead.

* Can theoretically work over any message-based transport,
  but default implementation uses websockets as firewall-friendly solution.
  Using websockets also brings some nice demultiplexing features as you
  can publish different methods on different HTTP endpoints.

pRpc does not use any interface definition files. Such approach has
it's disadvantages but is natural for dynamic languages as python.
On the positive side, it eliminates a lot of complexity with code generation
tooling and using the generated code.

Reference documentation
-----------------------

`Hosted on GitHub Pages <https://datadvance.github.io/pRpc/>`_

Contributing
------------

This project is developed and maintained by DATADVANCE LLC. Please
submit an issue if you have any questions or want to suggest an
improvement.

Acknowledgements
----------------

This work is supported by the Russian Foundation for Basic Research
(project No. 15-29-07043).
