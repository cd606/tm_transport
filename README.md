This is unstable software, with absolutely no warranty or guarantee whatsoever, whether express or implied, including but not limited to those of merchantability, fitness for specific purpose, or non-infringement.

This package builds upon tm_infra and tm_basic, and provides the following pre-packaged functionalities for codes written with those two packages:

* Tagging with UUID

* Communications through
  
  - RabbitMQ (via rabbitmq-c and SimpleAmqpClient libraries)

  - Multicast (via boost asio library)

  - ZeroMQ (via zmq and cppzmq libraries)

  - Redis (via hiredis library)

* Shared chain implemented with

  - Etcd3 (via Offscale libetcd library)

  - Redis (via hiredis library)

  - Shared memory (via boost interprocess library)

* Heartbeat and alert support, and service discovery through heartbeat

* Certain pre-defined node combinations