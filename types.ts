export const CHeader = "h:";
export const ICHeader = "i:";
export const ICHeaderLength = ICHeader.length;


export interface config {
    uri: string;
    auth: {
        username: string;
        password: string;
        passphrase: string;
    };
    database: string;
    path: string;
    initialScan: boolean;
    customChunkSize?: number;
    usePathObfuscation?: boolean;

}
export interface localConfig {
    path: string;
    initialScan: boolean;
    processor?: string;
}

export interface eachConf {
    server: config;
    local?: localConfig;
    group?: string;
    auto_reconnect?: boolean;
    sync_on_connect: boolean;

}

export interface configFile {
    [key: string]: eachConf;
}

export type status = {
    since: string;
    nodeId: string;
    lastProcessed: { [k: string]: number; }
}
export const statusDefault: status = {
    since: "",
    nodeId: Math.random().toString(36).slice(-10),
    lastProcessed: {}
}