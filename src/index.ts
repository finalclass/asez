const AerospikeNative = require('aerospike')
const Key = AerospikeNative.Key;

export class AerospikeSet {

    private client: any;
    private connectionPromise: Promise<any>;

    constructor(private config: any, private ns: string, private set: string) { }

    public async put(id: string, data): Promise<void> {
        let client = await this.getClient();
        let key = new Key(this.ns, this.set, id);
        if (typeof data.status !== 'string') {
            data.status = 'normal';
        }
        return await new Promise<void>((resolve, reject) => {
            client.put(key, data, function (err) {
                if (err) { reject(err); }
                else { resolve(); }
            })
        });
    }

    public async get(id: string): Promise<any> {
        let client = await this.getClient();
        let key = new Key(this.ns, this.set, id);
        return await new Promise<any>((resolve, reject) => {
            client.get(key, function (err, result, meta) {
                if (err) { reject(err); }
                else { resolve(result); }
            });
        });
    }

    public async readAll(): Promise<any[]> {
        let client = await this.getClient();
        return await new Promise<any[]>((resolve, reject) => {
            let query = client.query(this.ns, this.set);
            query.where(AerospikeNative.filter.equal('status', 'normal'));
            let stream = query.foreach();
            let records = [];
            stream.on('data', function (record) {
                records.push(record);
            });
            stream.on('error', function (err) {
                reject(err);
            })
            stream.on('end', function () {
                resolve(records);
            });
        });
    }

    public async close() {
        let client = await this.getClient();
        client.close();
    }

    public async remove(id: string) {
        return this.put(id, { status: 'removed' }, );
    }

    private async getClient() {
        if (!this.client && !this.connectionPromise) {
            this.connectionPromise = new Promise((resolve, reject) => {
                AerospikeNative.connect(this.config, (err, client) => {
                    if (err) { reject(err); }
                    else {
                        var options = {
                            ns: this.ns,
                            set: this.set,
                            bin: 'status',
                            index: 'idx_status',
                            datatype: AerospikeNative.indexDataType.STRING
                        }
                        client.createIndex(options, function (err, job) {
                            if (err) {
                                return reject(err);
                            }
                            job.waitUntilDone(function (err) {
                                if (err) {
                                    return reject(err);
                                }
                                resolve(client);
                            })
                        })

                    }
                })
            });
        }
        if (!this.client && this.connectionPromise) {
            this.client = await this.connectionPromise;
        }
        return this.client;
    }
}