tm_transport_sources = [
      'ConnectionLocator.cpp'
      , 'HeartbeatMessage.cpp'
      , 'AlertMessage.cpp'
      , 'HeartbeatAndAlertComponent.cpp'
      , 'multicast/MulticastComponent.cpp'
      , 'rabbitmq/RabbitMQComponent.cpp'
      , 'zeromq/ZeroMQComponent.cpp'
      , 'redis/RedisComponent.cpp'
      , 'nng/NNGComponent.cpp'
      , 'security/SignatureHelper.cpp'
      , 'shared_memory_broadcast/SharedMemoryBroadcastComponent.cpp'
]
tm_transport_lib = static_library('tm_kit_transport', tm_transport_sources, include_directories: inc, dependencies: common_deps, install: true)
pkg.generate(tm_transport_lib)