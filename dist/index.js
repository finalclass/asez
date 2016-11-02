"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator.throw(value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments)).next());
    });
};
const AerospikeNative = require('aerospike');
const Key = AerospikeNative.Key;
class AerospikeSet {
    constructor(config, ns, set) {
        this.config = config;
        this.ns = ns;
        this.set = set;
    }
    put(id, data) {
        return __awaiter(this, void 0, void 0, function* () {
            let client = yield this.getClient();
            let key = new Key(this.ns, this.set, id);
            if (typeof data.status !== 'string') {
                data.status = 'normal';
            }
            return yield new Promise((resolve, reject) => {
                client.put(key, data, function (err) {
                    if (err) {
                        reject(err);
                    }
                    else {
                        resolve();
                    }
                });
            });
        });
    }
    get(id) {
        return __awaiter(this, void 0, void 0, function* () {
            let client = yield this.getClient();
            let key = new Key(this.ns, this.set, id);
            return yield new Promise((resolve, reject) => {
                client.get(key, function (err, result, meta) {
                    if (err) {
                        reject(err);
                    }
                    else {
                        resolve(result);
                    }
                });
            });
        });
    }
    readAll() {
        return __awaiter(this, void 0, void 0, function* () {
            let client = yield this.getClient();
            return yield new Promise((resolve, reject) => {
                let query = client.query(this.ns, this.set);
                query.where(AerospikeNative.filter.equal('status', 'normal'));
                let stream = query.foreach();
                let records = [];
                stream.on('data', function (record) {
                    records.push(record);
                });
                stream.on('error', function (err) {
                    reject(err);
                });
                stream.on('end', function () {
                    resolve(records);
                });
            });
        });
    }
    close() {
        return __awaiter(this, void 0, void 0, function* () {
            let client = yield this.getClient();
            client.close();
        });
    }
    remove(id) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.put(id, { status: 'removed' });
        });
    }
    getClient() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.client && !this.connectionPromise) {
                this.connectionPromise = new Promise((resolve, reject) => {
                    AerospikeNative.connect(this.config, (err, client) => {
                        if (err) {
                            reject(err);
                        }
                        else {
                            var options = {
                                ns: this.ns,
                                set: this.set,
                                bin: 'status',
                                index: 'idx_status',
                                datatype: AerospikeNative.indexDataType.STRING
                            };
                            client.createIndex(options, function (err, job) {
                                if (err) {
                                    return reject(err);
                                }
                                job.waitUntilDone(function (err) {
                                    if (err) {
                                        return reject(err);
                                    }
                                    resolve(client);
                                });
                            });
                        }
                    });
                });
            }
            if (!this.client && this.connectionPromise) {
                this.client = yield this.connectionPromise;
            }
            return this.client;
        });
    }
}
exports.AerospikeSet = AerospikeSet;
//# sourceMappingURL=index.js.map