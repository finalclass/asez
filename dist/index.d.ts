export declare class AerospikeSet {
    private config;
    private ns;
    private set;
    private client;
    private connectionPromise;
    constructor(config: any, ns: string, set: string);
    put(id: string, data: any): Promise<void>;
    get(id: string): Promise<any>;
    readAll(): Promise<any[]>;
    close(): Promise<void>;
    remove(id: string): Promise<void>;
    private getClient();
}
