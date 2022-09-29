import {
    ApiPromise,
    WsProvider,
  } from "@polkadot/api";
import * as fs from 'fs';
import { stringify } from 'csv-stringify';
import { PubSub } from '@google-cloud/pubsub';


    
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

  catchEvent(block, decimals,from, to, amount, evt, extrinsicData) {
    const { event, phase } = evt;
    let extrinsicTimestamp = extrinsicData[0].method.args.now;
    extrinsicTimestamp = extrinsicTimestamp.split(",").join("");
    extrinsicTimestamp = parseInt(extrinsicTimestamp.substring(0, 10));
    let status = event.meta.docs.toHuman()[0] || "";
    let extrinsicIndex = phase.toHuman();
    extrinsicIndex = extrinsicIndex["ApplyExtrinsic"];

    return {
      block_timestamp: extrinsicTimestamp,
      extrinsic_index: block+"-"+extrinsicIndex,
      from: from.toString(),
      to: to.toString(),
      amount: this.asNumber(amount) / Math.pow(10, decimals),
      block_num: block,
      success: status !== "",
    }
  }

  async fetchTransfers(startBlock, endBlock, cb) {
    const decimals = await this.decimals();
  
    for (let i = startBlock; i <= endBlock; i++) {
      const blockHash = await this.api.rpc.chain.getBlockHash(i);
      const record = await this.api.derive.tx.events(blockHash);
      const signedBlock = await this.api.rpc.chain.getBlock(blockHash);
      const extrinsicData = signedBlock.block.extrinsics.toHuman();

      record.events.forEach((evt) => {
        const { event } = evt;
        const eventName = `${event.section}.${event.method}`;

        if (eventName === "allocations.NewAllocation") {
          const [to, amount] = event.data;
          let t = this.catchEvent(i, decimals, "0", to, amount, evt, extrinsicData);
          t["event_type"] = "allocations.NewAllocation";
          cb(t);
        }

        if (eventName === "balances.Transfer") {
          const [from, to, amount] = event.data;
          let t = this.catchEvent(i, decimals, from, to, amount, evt, extrinsicData);
          t["event_type"] = "balances.Transfer";
          cb(t);
        }
      });
      console.log(`Block ${i} scan success!`)
    }
    console.log(`Finished scan from ${startBlock} to ${endBlock}; total of ${endBlock-startBlock+1} blocks`)
  }
}

const build = async function() {
  const url = "wss://nodle-parachain.api.onfinality.io/public-ws";
  const wsProvider = new WsProvider(url);
  const api = await ApiPromise.create({ provider: wsProvider });
  return new Substrate(api);
}



let main = async () => {
  const MAX_SIZE = 100000;
  
  let projectId = "";
  let topicName = "";
  let startBlock = 0;
  let endBlock = 0;


  let action = "";
  if (process.argv.length >= 3) {
    action = process.argv[2];
  }
  
  
  if ((action == "csv" || action == "json") && process.argv.length >= 4) {
    startBlock = process.argv[3]
    endBlock = process.argv[3]
    console.log(`The execution will output into data.${action}`)
  }
  
  if (action == "pubsub" && process.argv.length >= 7) {
    projectId = process.argv[3]
    topicName = process.argv[4]
    startBlock = process.argv[5]
    endBlock = process.argv[6]
    console.log(`The execution will output into pubsub topic ${topicName} from project ${projectId}`)
  }
  
  console.log(`Querying a total of ${endBlock-startBlock+1} blocks from ${startBlock} to ${endBlock}`);
  
  let stream;
  if (action === "csv") {
    stream = stringify({ header: true, columns: [
      "block_num",
      "event_type",
      "block_timestamp",
      "extrinsic_index",
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
  
  const scanner = await build();
  let transfers = [];

  scanner.fetchTransfers(startBlock, endBlock, (transfer) => {
    if (action === "csv") {
      stream.write(transfer);
    }
  
    if (action === "json") {
      stream.write(JSON.stringify(transfer, null, 2)+"\r\n")
    }
  
    if (action === "pubsub") {
      transfers.push(transfer);
      
      if (transfers.length > MAX_SIZE) {
        // console.log(transfers.length);
        // const pubsub = new PubSub({projectId});
        // const topic = pubsub.topic(topicName);
        // let row = Buffer.from(transfers);
        // topic.publishMessage(row);
        // transfers = [];
      }
    }
  });
}


main();


