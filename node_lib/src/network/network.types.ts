export enum Transport {
    Multicast
    , RabbitMQ
    , Redis
    , ZeroMQ
    , NATS
}

export enum TopicMatchType {
    MatchAll
    , MatchExact
    , MatchRE
}

export interface TopicSpec {
    matchType: TopicMatchType
    , exactString: string
    , regex: RegExp | null
}

export interface ConnectionLocator {
    host: string
    , port: number
    , username: string
    , password: string
    , identifier: string
    , properties: Record<string, string>
}

export interface FacilityOutput {
    id: string
    , originalInput: Buffer
    , output: Buffer
    , isFinal: boolean
}

export interface ClientFacilityStreamParameters {
    address: string
    , userToWireHook?: (data: Buffer) => Buffer
    , wireToUserHook?: (data: Buffer) => Buffer
    , identityAttacher?: (data: Buffer) => Buffer
}

export interface ServerFacilityStreamParameters {
    address: string
    , userToWireHook?: (data: Buffer) => Buffer
    , wireToUserHook?: (data: Buffer) => Buffer
    , identityResolver?: (data: Buffer) => [boolean, string, Buffer]
}

export interface Heartbeat {
    uuid_str: string;
    timestamp: number;
    host: string;
    pid: number;
    sender_description: string;
    broadcast_channels: Record<string, string[]>;
    facility_channels: Record<string, string>;
    details: Record<string, { status: string, info: string }>;
}

export interface Alert {
    alertTime: number;
    host: string;
    pid: number;
    sender: string;
    level: string;
    message: string;
}

export interface OneTLSClientConfiguration {
    ca_cert: string;
    client_cert: string;
    client_key: string;
}

export interface ITLSClientConfigurationComponent {
    setTLSClientConfigurationItem(host: string, port: number, config: OneTLSClientConfiguration): void;
    getTLSClientConfigurationItem(host: string, port: number): OneTLSClientConfiguration | null;
}