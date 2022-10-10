import { ApiPromise, WsProvider } from '@polkadot/api'
import { encodeAddress } from '@polkadot/util-crypto'
import { BigQuery } from '@google-cloud/bigquery';


const nodleTestNode = 'wss://nodle-paradis.api.onfinality.io/public-ws'
const nodleProdNode = 'wss://nodle-parachain.api.onfinality.io/public-ws'
const nodleNode = nodleProdNode;

async function main() {
    // Substrate node we are connected to and listening to remarks
    const provider = new WsProvider(nodleNode);
    const api = await ApiPromise.create({ provider });

    // Get general information about the node we are connected to
    const [chain, nodeName, nodeVersion] = await Promise.all([
        api.rpc.system.chain(),
        api.rpc.system.name(),
        api.rpc.system.version()
    ]);
    console.log(
        `You are connected to chain ${chain} using ${nodeName} v${nodeVersion}`
    );


    const properties = await api.rpc.system.properties()
    const decimalst = properties.tokenDecimals.unwrapOr([12])[0]
    const decimals = parseInt(decimalst.toString());

    // Adjust how many accounts to query at once.
    let limit = 1000;
    let total = 0;
    let result = [];
    let last_key = ""

    const start = Date.now()

    while (true) {
        let query = await api.query.system.account.entriesPaged({ args: [], pageSize: limit, startKey: last_key });

        if (query.length == 0) {
            break
        }
        let batch = 0
        for (const user of query) {
            let account_id = encodeAddress(user[0].slice(-32), 37);
            let free_balance = user[1].data.free.toString();
            let reserved_balance = user[1].data.reserved.toString();
            result.push({ account_id: account_id, free_balance: free_balance / Math.pow(10, decimals), reserved_balance: reserved_balance / Math.pow(10, decimals) })
            last_key = user[0];
            batch++
        }
        total += batch
        console.log("fetched " + batch + ", total " + total);
    }
    const stop = Date.now()


    for (let i in result) {
        result[i]["receivedtime"] = stop;
        let r = result[i];
        result[i] = r
    }

    let dataset = "";
    let table = "";

    let bigquery = new BigQuery();
    await bigquery.dataset(dataset).table(table).insert(transfers);
    console.log(`Inserted ${transfers.length} rows`);
    bigquery = null;

    console.log(`Time Taken to execute = ${(stop - start) / 1000} seconds`);
}


main().catch((error) => {
    console.error(error);
    process.exit(-1);
});