****************
prpc.Connection
****************

.. automodule:: prpc.connection

.. autoclass:: prpc.connection.Connection
  :members:
  :exclude-members: call_unary, call_istream,
                    call_ostream, call_bistream, call_simple,
                    connect, accept, close

  .. autocomethod:: call_unary
    :async-with:

  .. autocomethod:: call_istream
    :async-with:

  .. autocomethod:: call_ostream
    :async-with:

  .. autocomethod:: call_bistream
    :async-with:

  .. autocomethod:: call_simple
    :coroutine:

  .. autocomethod:: connect
    :coroutine:

  .. autocomethod:: accept
    :coroutine:

  .. autocomethod:: close
    :coroutine:
