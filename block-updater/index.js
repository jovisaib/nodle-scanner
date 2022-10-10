import {
    ApiPromise,
    WsProvider,
  } from "@polkadot/api";
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

  catchEvent(block, decimals, from, to, amount, evt, extrinsic, extrinsicData) {
    const { event, phase } = evt;
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

  async fetchTransfers(startBlock, endBlock, maxBlockBatch, cb) {
    let NUM_BLOCKS = maxBlockBatch;
    const decimals = await this.decimals();
    let lastBlock = "none";
    let counter = 1;
    let transfers = [];

    for (let i = startBlock; i <= endBlock; i++) {  
      const blockHash = await this.api.rpc.chain.getBlockHash(i);
      const record = await this.api.derive.tx.events(blockHash);
      const signedBlock = await this.api.rpc.chain.getBlock(blockHash);
      let extrinsicData = signedBlock.block.extrinsics.toHuman();

      record.events.forEach((evt) => {
        const { event } = evt;
        const eventName = `${event.section}.${event.method}`;

        if (eventName == "balances.Transfer") {
          const [from, to, amount] = event.data;
          let t = this.catchEvent(i, decimals, from, to, amount, evt, signedBlock.block.extrinsics, extrinsicData);
          if (from == "4jbtsgNhpGAzdEGrKRb7g8Mq4ToNUpBVxeye942tWfG3gcYi") {
            t["event_type"] = "allocation";
          }else{
            t["event_type"] = "transfer";
          }
          transfers.push(t);
        }
      });

      if (transfers.length >= 1 && counter >= NUM_BLOCKS) {
        await cb(transfers);
        transfers = [];
        counter = 0;
        lastBlock = i;
      }
      console.log(`Block ${i} there are ${endBlock-i+1} left - last block stored was ${lastBlock}`)
      counter++;
    }

    if (transfers.length >= 1) {
      await cb(transfers);
    }
    console.log(`Finished scan from ${startBlock} to ${endBlock}; total of ${endBlock-startBlock+1} blocks`)
  }
}

const build = async function(url) {
  const wsProvider = new WsProvider(url);
  const api = await ApiPromise.create({ provider: wsProvider });
  return new Substrate(api);
}


let main = async () => {
  const DEFAULT_URL = "wss://nodle-parachain.api.onfinality.io/ws?apikey=245a89da-c9f1-47c8-801b-f7a27a122862";
  
  let startBlock = 0;
  let endBlock = 0;


  let maxBlockBatch = 120;
  
  
  

    startBlock = parseInt(process.argv[5]);
    endBlock = parseInt(process.argv[6]);

    console.log(`The execution will output into bq table ${table}`)
  }
  
  console.log(`Querying a total of ${endBlock-startBlock+1} blocks from ${startBlock} to ${endBlock}`);
  
  
  const scanner = await build(DEFAULT_URL);

  await scanner.fetchTransfers(startBlock, endBlock, maxBlockBatch, async transfers => {
    let bqDataset = "ble_event";
    let table = "nodl_transfers_2022sept";
    let bigquery = new BigQuery();
    await bigquery.dataset(bqDataset).table(table).insert(transfers);
    console.log(`Inserted ${transfers.length} rows`);
    bigquery = null;
  });

main();


