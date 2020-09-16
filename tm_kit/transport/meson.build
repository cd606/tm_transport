tm_transport_headers = [
      'ConnectionLocator.hpp'
      , 'ByteDataHook.hpp'
      , 'BoostUUIDComponent.hpp'
      , 'AbstractIdentityCheckerComponent.hpp'
      , 'EmptyIdentityCheckerComponent.hpp'
      , 'SimpleIdentityCheckerComponent.hpp'
      , 'HeartbeatMessage.hpp'
      , 'AlertMessage.hpp'
      , 'HeartbeatAndAlertComponent.hpp'
      , 'MultiTransportBroadcastListener.hpp'
      , 'MultiTransportRemoteFacility.hpp'
      , 'HeartbeatMessageToRemoteFacilityCommand.hpp'
      , 'MultiTransportRemoteFacilityManagingUtils.hpp'
      , 'MultiTransportBroadcastListenerManagingUtils.hpp'
      , 'CrossGuidComponent.hpp'
    ]
  
install_headers(tm_transport_headers, subdir : 'tm_kit/transport')
subdir('multicast')
subdir('rabbitmq')
subdir('zeromq')
subdir('redis')
subdir('nng')
subdir('etcd_shared_chain')
subdir('lock_free_in_memory_shared_chain')