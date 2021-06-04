# Ensuring Causal Consistency

## causal-metadata
For our causal-metadata, we pass in an array of length 4: [R1, R2, R3, P]
- [R1, R2, R3] represent the Vector Clock values for each of our three replicas
- P indicates what the position of the sender replica is
  -  For example, if Replica 1 was sending its VC [2, 0, 2], causal-metadata would be [2, 0, 2, 0]

## Causal Broadcasts
In order to keep consistent replicas, whichever process serves a request from the client (that updates the key-value store) sends this update to the other replicas. These are PUT and DELETE requests.

We implemented broadcast as a series of unicast messages:
- The sender replica will increment its own position in its VC (since a broadcast is one event), and then send the message to the other replicas on a special replica-to-replica endpoint called */key-value-store-r/*
```
sendersVC[senderposition]++
senderVC[3] = sender position
```

## canDeliver() Algorithm
In order to determine whether a replica can deliver the message it has just recieved, we use the Causal Broadcast Algorithm from Lecture 5, pseudocode below

```
canDeliver(sender vector clock (senderVC), reciever vector clock (thisVC)):

  if the conditions are met, return true:
    senderVC[sender slot] = thisVC[sender slot] + 1
    senderVC[every not sender slot] <= thisVC[every not sender slot]
    
  else return false
```

## Causal Delivery
When a replica recieves a message, it uses the `canDeliver()` conditions to determine if delivering this message would violate causal consistency.

If it is safe, the replica updates its vector clock: max (sender, current) for all values, and delivers the message. It then returns this updated VC and a success message to the sender replica at the replica-only endpoint
  - Note that we do not increment the deliver VC position, since we are not counting delivers as events.
  - In a sense, our VC implementation serves as a timestamp of send messages from each replica. 
    - EG: Let's say replica 1 sends two messages (with VC entries 1 and 2). 
    - If replica 2 recieves message 2 first, its current VC entry for replica 1 will be 0
    - Since 0 + 1 != 2, it knows that it is waiting on one message before it can deliver this one
    - When it recieves the message with VC entry 1, it can deliver (0 + 1 = 1) and update its own VC
    - It can then deliver 2, since 1 + 1 = 2

If, however, it is not safe, then we know this replica is not up to date. We will set the replica status to indicate it is not up to date. While a replica is not up to date, it is blocked from serving any client requests. We call kvGet() to the other replicas, which will return an updated version of the key-value store. Once we ahev updated our kvStore, we set the status to up-to-date again. Once unblocked, this replica can now serve requests from the client that it knows will be consistent.

# Detecting whether a node is currently up or not
On a 1 second interval timer, on a separate thread, we perform a health check. The purpose of the health check is to
broadcast GET requests to all other node's. If node(s) are down, in the GET request functionality, we add the failed to receive GET response socket address(es) to an array local to each replica. After broadcasting the GET requests, using the information gathered in the array, we can broadcast DELETE requests using each socket address in the array in turn. In the case in which all replica's are down, we delete the replica(s) from our personal view. Otherwise if at least two replica's are currently up, then before sending a response back, the receiver replica deletes the socket address from its own view received in the DELETE request. The operations involved with the health check are performed indefinitely to ensure that all replica's know as soon as possible when a replica goes down or when it comes back up. In the case in which we have a false positive, we just consider the replica to be dead and therefore remove it from all other replica's view. In the scenario in which we get a false negative, when the receiver replica goes down after sending its view, all other nodes will learn of this is the next round of broadcast GET requests. Therefore, the false negative won't exist for long and will correct itself soon after. Here is a demonstration of how the health check operates when a replica is down:

![replica2 is down](src/replica_down.png "replica down")



# Resharding Process

When a node is given a request to the `/key-value-store-shard/reshard` endpoint, we first determine if it is a valid reshard request by checking if the amount of total replicas divided by the new shard count is greater than 2 (so we can have at least 2 replicas in each shard). the new set of shards given our shard count and the current replicas that are up. The way we determine which replica is in which shard is a simple sort and split.

First we sort all the replicas that are available, and from there we divide them into `N` subarrays where `N` is the number of shards given in the request to our node. We make sure each subarray (shard) has an "even split" amount of replicas in it, then we move to the next one. The "even split" is the amount of replicas divided by the number of shards, so we can get as close to an even split possible for all the shards.

If there are any extra replicas after we have given at our even split of replicas to each shard, the extras are put in the last shard in the list. For example, if we had 10 replicas with a `shard-count` of 3, then the replica distribution would look like this: (3,3,4).  The node then sends out those updates along the `/key-value-store-shard/updatesm` endpoint to update the Shard Members across all replicas. 

After we update the shard members, we shard our keys across all nodes, which is described in the section below.

# Sharding Keys Across Nodes

In this assignment, we used the following hashing function from a Go package to determine which shard a key value pair will hash into:
```
func HashModN(s string, n int) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() % uint32(n))
}
```

This function takes a string `s` and an int `n`, where `s` is the value to be hashed (our key), and `n` is the amount of shards we have. This is based off of the sharding approach we learned in class where we hash the key value, then take the mod of the hashed key by the amount of shards to determine what shard our key ends up in. We have tested this approach rigourously and it gives us a pretty even split in keys to shards.

We use the `hash/fnv` library from the Go standard library. We implement a 32 bit FNV-1 hash and use that to hash our keys. The FNV hash is the Fowler–Noll–Vo hash function. We chose to use it since it was easy to implement in Go, it's relatively fast, and it has a low collision rate.

For example, in order to find the shardID that corresponds to a key, we do:
```
thisKeysShardID := HashModN(key, numberOfShards)
```

This function is used to reshard keys, as well as to determine which shard a key should be in for serving KV requests (GET, PUT, DELETE).
- When resharding, a coordinator node iterates through all keys and assigns them a new "chunk" based on which shard it belongs to. These chunks are basically the KVStore for each shard. Once all keys have been assigned to a chunk, the coordinator node broadcasts each replica in a given shard its new chunk, which becomes the new KVStore for said replica and all replicas in that shard.
- For KV requests, the replica that serves the request first determines which shard the key belongs to. If it is its own shardID, it serves the request and returns to the client. If instead, it belongs to a different shard, it will forward the request to the first available replica in the correct shard.

The drawbacks to using this approach is that if our coordinator node fails/goes down during resharding, then our whole process will fail. We deemed that pretty unlikely, and our resharding process happens fast enough so that it didn't seem like a large enough issue to work around for this case. It would be much more complicated to deal with doing a resharding process across multiple nodes versus having one do all the work and distribute the correct data as well.
