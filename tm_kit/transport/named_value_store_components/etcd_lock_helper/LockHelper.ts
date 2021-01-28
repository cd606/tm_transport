import { Etcd3 } from 'etcd3'
import * as yargs from 'yargs'
import { setTimeout } from 'timers';

(async () => {
    const LOCK_NUMBER_KEY = `${yargs.argv.storagePrefix}:${yargs.argv.lockNumberKey}`;
    const LOCK_QUEUE_KEY = `${yargs.argv.storagePrefix}:${yargs.argv.lockQueueKey}`;
    if (yargs.argv.init !== undefined) {
        const client = new Etcd3();
        await client.stm().transact(tx => {
            return Promise.all([
                tx.delete().key(LOCK_NUMBER_KEY).exec()
                , tx.delete().key(LOCK_QUEUE_KEY).exec()
            ]);
        });
        console.log("Lock initialized");
    } else if (yargs.argv.show !== undefined) {
        const client = new Etcd3();
        let info = await client.stm().transact(tx => {
            return Promise.all([
                tx.get(LOCK_NUMBER_KEY).exec()
                , tx.get(LOCK_QUEUE_KEY).exec()
            ]).then(([num,q]) => {
                let numVersion = 0;
                if (num.kvs.length > 0) {
                    numVersion = parseInt(num.kvs[0].version);
                }
                let queueVersion = 0;
                if (q.kvs.length > 0) {
                    queueVersion = parseInt(q.kvs[0].version);
                }
                return Promise.all([numVersion, queueVersion]);
            })
        });
        console.log(`The next waiting number is ${info[0]} and the next serving number is ${info[1]}`);
    } else if (yargs.argv.lock !== undefined) {
        const client = new Etcd3();
        let numVersion = 0;
        while (true) {
            numVersion = 0;
            let num = await client.get(LOCK_NUMBER_KEY).exec();
            if (num.kvs.length > 0) {
                numVersion = parseInt(num.kvs[0].version);
            }
            let success = await client.if(LOCK_NUMBER_KEY, "Version", "==", numVersion)
                .then(client.put(LOCK_NUMBER_KEY))
                .commit()
                .then(result => {
                    return Promise.resolve(result.succeeded);
                });
            if (success) {
                console.log(`My number is ${numVersion}`);
                break;
            }
            await new Promise(r => setTimeout(r, 100));
        }
        while (true) {
            let queueVersion = 0;
            let q = await client.get(LOCK_QUEUE_KEY).exec();
            if (q.kvs.length > 0) {
                queueVersion = parseInt(q.kvs[0].version);
            }
            if (queueVersion == numVersion) {
                break;
            }
            await new Promise(r => setTimeout(r, 100));
        }
        console.log(`Lock obtained for ${numVersion}`);
    } else if (yargs.argv.unlock !== undefined) {
        const client = new Etcd3();
        let queueVersion = 0;
        while (true) {
            queueVersion = 0;
            let q = await client.get(LOCK_QUEUE_KEY).exec();
            if (q.kvs.length > 0) {
                queueVersion = parseInt(q.kvs[0].version);
            }
            let success = await client.if(LOCK_QUEUE_KEY, "Version", "==", queueVersion)
                .then(client.put(LOCK_QUEUE_KEY))
                .commit()
                .then(result => {
                    return Promise.resolve(result.succeeded);
                });
            if (success) {
                ++queueVersion;
                console.log(`After unlock, ready to serve ${queueVersion}`);
                break;
            }
            await new Promise(r => setTimeout(r, 100));
        }
    }
})();