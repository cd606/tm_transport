This is unstable software, with absolutely no warranty or guarantee whatsoever, whether express or implied, including but not limited to those of merchantability, fitness for specific purpose, or non-infringement.

This package builds upon tm_infra and tm_basic, and provides the following pre-packaged functionalities for codes written with those two packages:

* Tagging with UUID

* Communications through
  
  - RabbitMQ (via rabbitmq-c library)

  - Multicast (via boost asio library)

  - ZeroMQ (via zmq and cppzmq libraries)

  - Redis (via hiredis library)

  - NNG (via libnng library)

  - Shared memory broadcast (via boost interprocess library)

  - Direct socket connection (via boost asio library)

  - JSON (via boost beast and/or curlpp)

  - Websocket (via boost beast)

* Shared chain implemented with

  - Etcd3 (via Offscale libetcd library)

  - Redis (via hiredis library)

  - Shared memory (via boost interprocess library)

* Heartbeat and alert support, and service discovery through heartbeat

* Certain pre-defined node combinations

* Digital signature based identity attaching in facility calls

INSTALLATION NOTES:

The requirements of tm_transport are, in addition to requirements of tm_infra and tm_basic:

* tm_infra and tm_basic

* boost >= 1.73.0

* librabbitmq-c (https://github.com/alanxz/rabbitmq-c)

* libzmq

* CppZmq (https://github.com/zeromq/cppzmq)

* hiredis

* libnng

* nngpp (https://github.com/cwzx/nngpp)

* CrossGuid (https://github.com/graeme-hill/crossguid)

* Offscale Etcd C++ client library (https://github.com/offscale/libetcd-cpp)

* libsodium

* boost_certify (https://github.com/djarek/certify) 

* curlpp

Most of these can be installed through vcpkg. After installing the packages through vcpkg, sometimes a hand-written pkg-config file might be needed for meson to find the package, especially on Windows.

(boost_certify is not on vcpkg, but it is a header-only library so a hand-written pkg-config file is easy to create.)

On Ubuntu 18.04 at least, if Offscale Etcd C++ client library is built through vcpkg using the default toolchain (gcc), then clang will not be able to use the library in linking. However, if it is build through vcpkg using a customized toolchain that uses clang (11.0.0 tested), then both clang and gcc (10.1.0 tested) will be able to use it in the linker. 

The Typescript code included in this package has been tested with Nodejs 14.12.0 and Typescript 3.9.3. (For possible issues with etcd3 package, please refer to the comment at the beginning of TMTransport_Chains.ts)

The Python code included in this package has been tested with Python 3.9.0rc1.

## JSON REST event streams

`JsonRESTComponent` can serve static files, JSON handlers, and Server-Sent Events on the same port. Register an SSE GET path with `registerSSEPublisher(locator)` and send an already encoded payload with `publishSSE(locator, payload, eventName)`. The transport frames the event, caches the latest one for new subscribers, sends heartbeat comments, and disconnects clients whose pending output exceeds 1 MiB. `JsonRESTSSEPublisher<R>::create(r, name, locator, eventName)` provides an AppRunner sink for `std::string` payloads.

Call `setBindAddress(port, address)` before the listener starts to limit which local IP receives connections. The default remains `0.0.0.0`; the address applies to every path on that port. Existing `setDocRoot` and `registerHandler` calls can use the same port as the SSE publisher.

`JsonRESTSSEExporter<Env>` provides typed and pre-encoded tm-kit exporters. For an AppRunner broadcast sink, use `json_rest_sse://:23457:::/api/events[event=snapshot]` as the channel specification in `MultiTransportBroadcastPublisherManagingUtils` or `PublisherTouchupSpec`. `oneBroadcastPublisher<T>` JSON-encodes a JSON-wrappable `T`; `oneByteDataBroadcastPublisher` sends its content as already encoded text. The `event` locator property sets the SSE event name. Without it, the input topic is the event name, or `message` if the topic is empty. Protocol-based publisher helpers also use JSON encoding for `json_rest_sse://` channels. Different paths can share one IP and port with each other, JSON handlers, and static files. The locator host does not set the listener bind address; use `setBindAddress` for that.
