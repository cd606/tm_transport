import { IOptions as IEtcd3Options } from 'etcd3';

export interface EtcdSharedChainConfiguration {
    etcd3Options: IEtcd3Options
    , headKey: string
    , saveDataOnSeparateStorage: boolean
    , chainPrefix: string
    , dataPrefix: string
    , extraDataPrefix: string
    , redisServerAddr: string
    , duplicateFromRedis: boolean
    , redisTTLSeconds: number
    , automaticallyDuplicateToRedis: boolean
}