tm_transport_headers = [
      'ConnectionLocator.hpp'
      , 'ByteDataHook.hpp'
      , 'BoostUUIDComponent.hpp'
      , 'AbstractIdentityCheckerComponent.hpp'
      , 'EmptyIdentityCheckerComponent.hpp'
      , 'SimpleIdentityCheckerComponent.hpp'
    ]
  
install_headers(tm_transport_headers, subdir : 'tm_kit/transport')
subdir('multicast')
subdir('rabbitmq')
subdir('zeromq')
if get_option('redis').enabled()
  subdir('redis')
endif