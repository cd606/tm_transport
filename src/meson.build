tm_transport_sources = [
      'ConnectionLocator.cpp'
      , 'multicast/MulticastComponent.cpp'
      , 'rabbitmq/RabbitMQComponent.cpp'
      , 'zeromq/ZeroMQComponent.cpp'
]
tm_transport_lib = static_library('tm_kit_transport', tm_transport_sources, include_directories: inc, dependencies: common_deps, install: true)
pkg.generate(tm_transport_lib)