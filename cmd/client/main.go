package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "dynamodb1/dynamodb1/dynamodb1/proto"
	pb2 "dynamodb1/dynamodb1/proto"

	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", "localhost:7001", "grpc server address host:port")
	svc := flag.String("svc", "coord", "service to call: coord or replica")
	cmd := flag.String("cmd", "put", "command: put/get")
	key := flag.String("key", "k1", "key")
	val := flag.String("val", "v1", "value for put")
	timeout := flag.Int("timeout", 3, "rpc timeout seconds")
	flag.Parse()

	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeout)*time.Second)
	defer cancel()

	switch *svc {
	case "coord", "coordinator":
		coordClient := pb2.NewCoordinatorClient(conn)
		if *cmd == "put" {
			req := &pb2.CoordinatorPutReq{
				Key:   *key,
				Value: []byte(*val),
				// W left zero => coordinator default W
			}
			resp, err := coordClient.Put(ctx, req)
			if err != nil {
				log.Fatalf("coordinator put rpc error: %v", err)
			}
			if resp.Err != "" {
				log.Fatalf("coordinator put error: %s", resp.Err)
			}
			fmt.Println("coordinator put ok")
			if len(resp.StoredContexts) > 0 {
				fmt.Printf("stored contexts: %d\n", len(resp.StoredContexts))
				for i, c := range resp.StoredContexts {
					fmt.Printf(" ctx[%d] origin=%s tomb=%v ts=%d vc_entries=%d\n", i, c.Origin, c.Tombstone, c.Ts, len(c.Entries))
					for _, e := range c.Entries {
						fmt.Printf("   - %s:%d\n", e.Node, e.Counter)
					}
				}
			}
		} else {
			// get
			req := &pb2.CoordinatorGetReq{Key: *key}
			resp, err := coordClient.Get(ctx, req)
			if err != nil {
				log.Fatalf("coordinator get rpc error: %v", err)
			}
			if resp.Err != "" {
				log.Fatalf("coordinator get error: %s", resp.Err)
			}
			fmt.Printf("coord get: %d items\n", len(resp.Items))
			for i, it := range resp.Items {
				valStr := "<nil>"
				if it.Value != nil {
					valStr = string(it.Value)
				}
				if it.Ctx != nil {
					fmt.Printf(" item[%d] key=%s val=%s tomb=%v origin=%s ts=%d vc=%d\n", i, it.Key, valStr, it.Ctx.Tombstone, it.Ctx.Origin, it.Ctx.Ts, len(it.Ctx.Entries))
					for _, e := range it.Ctx.Entries {
						fmt.Printf("    - %s:%d\n", e.Node, e.Counter)
					}
				} else {
					fmt.Printf(" item[%d] key=%s val=%s (no ctx)\n", i, it.Key, valStr)
				}
			}
		}

	case "replica":
		// direct replica calls (node-to-node). Useful for admin or testing.
		repClient := pb.NewDynamoClient(conn)
		if *cmd == "put" {
			req := &pb.ReplicaPutReq{
				Item: &pb.ItemProto{
					Key:   *key,
					Value: []byte(*val),
					// leaving Item.Ctx empty signals coordinator behavior in prior heuristic,
					// but when calling replica directly you may want to set Ctx entries for replica-to-replica.
				},
			}
			resp, err := repClient.ReplicaPut(ctx, req)
			if err != nil {
				log.Fatalf("replica put rpc error: %v", err)
			}
			if resp.Err != "" {
				log.Fatalf("replica put error: %s", resp.Err)
			}
			fmt.Println("replica put ok")
			if len(resp.Stored) > 0 {
				fmt.Printf("stored items returned: %d\n", len(resp.Stored))
				for i, it := range resp.Stored {
					valStr := "<nil>"
					if it.Value != nil {
						valStr = string(it.Value)
					}
					fmt.Printf(" item[%d] key=%s val=%s\n", i, it.Key, valStr)
				}
			}
		} else {
			// get
			req := &pb.ReplicaGetReq{Key: *key}
			resp, err := repClient.ReplicaGet(ctx, req)
			if err != nil {
				log.Fatalf("replica get rpc error: %v", err)
			}
			if resp.Err != "" {
				log.Fatalf("replica get error: %s", resp.Err)
			}
			fmt.Printf("replica get: %d items\n", len(resp.Items))
			for i, it := range resp.Items {
				valStr := "<nil>"
				if it.Value != nil {
					valStr = string(it.Value)
				}
				if it.Ctx != nil {
					fmt.Printf(" item[%d] key=%s val=%s tomb=%v origin=%s ts=%d vc_entries=%d\n", i, it.Key, valStr, it.Ctx.Tombstone, it.Ctx.Origin, it.Ctx.Ts, len(it.Ctx.Entries))
					for _, e := range it.Ctx.Entries {
						fmt.Printf("   - %s:%d\n", e.Node, e.Counter)
					}
				} else {
					fmt.Printf(" item[%d] key=%s val=%s (no ctx)\n", i, it.Key, valStr)
				}
			}
		}

	default:
		log.Fatalf("unknown svc %q; valid values: coord, replica", *svc)
	}
}