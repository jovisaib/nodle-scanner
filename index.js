import {
    ApiPromise,
    WsProvider,
  } from "@polkadot/api";
import * as fs from 'fs';
import { stringify } from 'csv-stringify';
import { BigQuery } from '@google-cloud/bigquery';


    
class Substrate {
  constructor(api) {
    this.api = api;
  }

  asNumber(nb) {
    return parseInt(nb.toString());
  }

  async decimals(){
    const properties = await this.api.rpc.system.properties();
    const decimals = properties.tokenDecimals.unwrapOr([12])[0];
    return this.asNumber(decimals);
  }

  catchEvent(block, decimals, from, to, amount, evt, extrinsic) {
    const { event, phase } = evt;
    let extrinsicData = extrinsic.toHuman();
    let extrinsicTimestamp = extrinsicData[0].method.args.now;
    extrinsicTimestamp = extrinsicTimestamp.split(",").join("");
    extrinsicTimestamp = parseInt(extrinsicTimestamp.substring(0, 10));
    let status = event.meta.docs.toHuman()[0] || "";
    let extrinsicIndex = phase.toHuman();
    extrinsicIndex = extrinsicIndex["ApplyExtrinsic"];
    let extrinsicHash = extrinsic[extrinsicIndex].hash.toHex();

    return {
      block_timestamp: extrinsicTimestamp,
      extrinsic_index: block+"-"+extrinsicIndex,
      extrinsic_hash: extrinsicHash.toString(),
      from: from.toString(),
      to: to.toString(),
      amount: this.asNumber(amount) / Math.pow(10, decimals),
      block_num: block,
      success: status != "",
    }
  }

  async fetchTransfers(startBlock, endBlock, cb) {
    const decimals = await this.decimals();
  
    for (let i = startBlock; i <= endBlock; i++) {
      const blockHash = await this.api.rpc.chain.getBlockHash(i);
      const record = await this.api.derive.tx.events(blockHash);
      const signedBlock = await this.api.rpc.chain.getBlock(blockHash);

      record.events.forEach((evt) => {
        const { event } = evt;
        const eventName = `${event.section}.${event.method}`;

        if (eventName == "balances.Transfer") {
          const [from, to, amount] = event.data;
          let t = this.catchEvent(i, decimals, from, to, amount, evt, signedBlock.block.extrinsics);
          if (from == "4jbtsgNhpGAzdEGrKRb7g8Mq4ToNUpBVxeye942tWfG3gcYi") {
            t["event_type"] = "allocation";
          }else{
            t["event_type"] = "transfer";
          }
          cb(t);
        }
      });
      console.log(`Block ${i} scan success! ${endBlock-i+1}`)
    }
    console.log(`Finished scan from ${startBlock} to ${endBlock}; total of ${endBlock-startBlock+1} blocks`)
  }
}

const build = async function(url) {
  const wsProvider = new WsProvider(url);
  const api = await ApiPromise.create({ provider: wsProvider });
  return new Substrate(api);
}


function insertToBigQuery(rows, dataset, table) {
  const bigquery = new BigQuery();

  async function insertRowsAsStream() {
    console.log(dataset, table)
    await bigquery
      .dataset(dataset)
      .table(table)
      .insert(rows).catch(function(reason) {
        console.log(reason);
      });
    console.log(`Inserted ${rows.length} rows`);
  }
  insertRowsAsStream(rows);
}




let main = async () => {
  const DEFAULT_URL = "wss://nodle-parachain.api.onfinality.io/ws?apikey=245a89da-c9f1-47c8-801b-f7a27a122862";
  const MAX_SIZE = 300;
  
  let nodeUrl = DEFAULT_URL;
  let projectId = "";
  let dataset = "";
  let table = "";
  let startBlock = 0;
  let endBlock = 0;


  let action = "";
  if (process.argv.length >= 3) {
    action = process.argv[2];
  }
  
  
  if ((action == "csv" || action == "json") && process.argv.length >= 5) {
    startBlock = process.argv[3];
    endBlock = process.argv[4];
    if (process.argv.length == 6) {
      nodeUrl = process.argv[5];
    }
    console.log(`The execution will output into data.${action}`)
  }
  
  if (action == "pubsub" && process.argv.length >= 7) {
    dataset = process.argv[3];
    table = process.argv[4];
    startBlock = process.argv[5];
    endBlock = process.argv[6];

    if (process.argv.length == 8) {
      nodeUrl = process.argv[7];
    }
    console.log(`The execution will output into bq table ${table} from project ${projectId}`)
  }
  
  console.log(`Querying a total of ${endBlock-startBlock+1} blocks from ${startBlock} to ${endBlock}`);
  
  let stream;
  if (action === "csv") {
    stream = stringify({ header: true, columns: [
      "block_num",
      "event_type",
      "block_timestamp",
      "extrinsic_index",
      "extrinsic_hash",
      "from",
      "to",
      "amount",
      "success",
    ]});
    stream.pipe(fs.createWriteStream("./data.csv"));
  }
  
  if (action === "json") {
    stream = fs.createWriteStream("./data.json")
  }
  
  const scanner = await build(nodeUrl);
  let transfers = [];

  await scanner.fetchTransfers(startBlock, endBlock, async (transfer) => {
    if (action == "csv") {
      stream.write(transfer);
    }
  
    if (action == "json") {
      stream.write(JSON.stringify(transfer, null, 2)+"\r\n")
    }
  
    if (action == "pubsub") {
      transfers.push(transfer);
      if (transfers.length >= MAX_SIZE) {
        insertToBigQuery(transfers,dataset,table);
        transfers = [];
      }
    }
  });
}

main();


