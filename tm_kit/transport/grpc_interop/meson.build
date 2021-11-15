tm_transport_grpc_version_info_hpp = custom_target(
  'GrpcVersionInfo.hpp'
  , input: []
  , output: ['GrpcVersionInfo.hpp']
  , depends : tm_transport_update_grpc_version_exec
  , command: [
    tm_transport_update_grpc_version_exec
    , '@OUTPUT@'
  ]
  , install: true
  , install_dir: get_option('includedir')+'/tm_kit/transport/grpc_interop'
)

tm_transport_grpc_interop_headers = [
    'GrpcInteropComponentException.hpp',
    'GrpcInteropComponent.hpp',
    'GrpcServiceInfo.hpp',
    'GrpcConnectionLocatorUtils.hpp',
    'GrpcSerializationHelper.hpp',
    'GrpcClientFacility.hpp',
    'GrpcServerFacility.hpp'
  ]

install_headers(tm_transport_grpc_interop_headers, subdir : 'tm_kit/transport/grpc_interop')