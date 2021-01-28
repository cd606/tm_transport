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
      , 'MultiTransportFacilityWrapper.hpp'
      , 'MultiTransportBroadcastListenerManagingUtils.hpp'
      , 'MultiTransportBroadcastPublisherManagingUtils.hpp'
      , 'CrossGuidComponent.hpp'
      , 'AbstractHookFactoryComponent.hpp'
      , 'SharedChainCreator.hpp'
      , 'ExitDataSource.hpp'
      , 'RemoteTransactionSubscriberManagingUtils.hpp'
    ]
  
install_headers(tm_transport_headers, subdir : 'tm_kit/transport')
subdir('multicast')
subdir('rabbitmq')
subdir('zeromq')
subdir('redis')
subdir('nng')
subdir('etcd_shared_chain')
subdir('redis_shared_chain')
subdir('lock_free_in_memory_shared_chain')
subdir('security')
subdir('named_value_store_components')