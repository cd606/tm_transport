tm_transport_sources = [
      'ConnectionLocator.cpp'
      , 'HeartbeatMessage.cpp'
      , 'AlertMessage.cpp'
      , 'HeartbeatAndAlertComponent.cpp'
      , 'multicast/InterfaceToIP.cpp'
      , 'multicast/MulticastComponent.cpp'
      , 'rabbitmq/RabbitMQComponent.cpp'
      , 'zeromq/ZeroMQComponent.cpp'
      , 'redis/RedisComponent.cpp'
      , 'nng/NNGComponent.cpp'
      , 'security/SignatureHelper.cpp'
      , 'shared_memory_broadcast/SharedMemoryBroadcastComponent.cpp'
      , 'socket_rpc/SocketRPCComponent.cpp'
      , 'grpc_interop/GrpcInteropComponent.cpp'
      , 'grpc_interop/GrpcConnectionLocatorUtils.cpp'
      , 'json_rest/JsonRESTComponent.cpp'
      , 'websocket/WebSocketComponent.cpp'
      , 'HostNameUtil.cpp'
      , 'ConvertChainIDStringToGroup.cpp'
      , tm_transport_grpc_version_info_hpp
      , 'singlecast/SinglecastComponent.cpp'
      , 'BoostCertifyAdaptor.cpp'
]
if get_option('buildtype') == 'debug'
      tm_transport_lib_debug = static_library('tm_kit_transport_debug', tm_transport_sources, include_directories: inc, dependencies: common_deps, install: true)
      pkg.generate(tm_transport_lib_debug)
else
      tm_transport_lib = static_library('tm_kit_transport', tm_transport_sources, include_directories: inc, dependencies: common_deps, install: true)
      pkg.generate(tm_transport_lib)
endif