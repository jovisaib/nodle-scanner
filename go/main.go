package main

import (
	"fmt"

	gsrpc "github.com/centrifuge/go-substrate-rpc-client/v4"
	"github.com/centrifuge/go-substrate-rpc-client/v4/types"
)

func main() {
	api, err := gsrpc.NewSubstrateAPI("wss://nodle-parachain.api.onfinality.io/public-ws")
	if err != nil {
		fmt.Println("API ERR: ", err)
	}

	hash, err := api.RPC.Chain.GetBlockHash(304864)
	if err != nil {
		fmt.Println("ERR: ", err)
	}

	// signedBlock, err := api.RPC.Chain.GetBlock(hash)
	// if err != nil {
	// 	fmt.Println("ERR: ", err)
	// }

	// fmt.Println(signedBlock.Block.Extrinsics)

	meta, err := api.RPC.State.GetMetadata(hash)
	if err != nil {
		panic(err)
	}

	key, err := types.CreateStorageKey(meta, "System", "Events", nil)
	if err != nil {
		panic(err)
	}

	sub, err := api.RPC.State.SubscribeStorageRaw([]types.StorageKey{key})
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	// outer for loop for subscription notifications
	for {
		set := <-sub.Chan()
		// inner loop for the changes within one of those notifications
		for _, chng := range set.Changes {
			// Decode the event records
			events := types.EventRecords{}
			err = types.EventRecordsRaw(chng.StorageData).DecodeEventRecords(meta, &events)
			if err != nil {
				panic(err)
			}

			for _, e := range events.Balances_Transfer {
				fmt.Printf("\tBalances:Transfer:: (phase=%#v)\n", e.Phase)
				fmt.Printf("\t\t%v, %v, %v\n", e.From, e.To, e.Value)
			}

		}
	}
}
