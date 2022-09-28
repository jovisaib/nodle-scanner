import {
    ApiPromise,
    WsProvider,
  } from "@polkadot/api";
import * as fs from 'fs';
import { stringify } from 'csv-stringify';
import {PubSub} from '@google-cloud/pubsub';




    
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

  async fucurrentBlocknction() {
    return this.asNumber(await this.api.query.system.number());
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

  async fetchTransfers(
    startBlock,
    endBlock,
    cb,
  ) {
  const total = endBlock - startBlock;
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
  }
}
}

const build = async function() {
  const url = "wss://nodle-parachain.api.onfinality.io/public-ws";
  const wsProvider = new WsProvider(url);
  const api = await ApiPromise.create({ provider: wsProvider });
  return new Substrate(api);
}

const projectId = 'your-project-id';
const topicName = 'my-topic';

const startBlock = 552869;
const endBlock = 552869;
const scanner = await build();
const maxSize = 100000;
let transfers = [];



const writeCsv = true;
const csvFilename = "data.csv";
const columns = [
  "block_num",
  "event_type",
  "block_timestamp",
  "extrinsic_index",
  "from",
  "to",
  "amount",
  "success",
];
const stringifier = stringify({ header: true, columns: columns });
stringifier.pipe(fs.createWriteStream(  "./"+csvFilename ));

const jsonFilename = "data.json";
const writeJson = true;
var jsonStream = fs.createWriteStream("./"+jsonFilename)


await scanner.fetchTransfers(startBlock, endBlock, (transfer) => {
  transfers.push(transfer);
  console.log(transfers.length);

  if (writeCsv) {
    stringifier.write(transfer);
  }

  if (writeJson) {
    jsonStream.write(JSON.stringify(transfer, null, 2)+"\r\n")
  }
  // if (transfers.length > maxSize) {
    // console.log(transfers.length);
    // const pubsub = new PubSub({projectId});
    // const topic = pubsub.topic(topicName);
    // let row = Buffer.from(transfers);
    // topic.publishMessage(row);
    // transfers = [];
    // }
});
