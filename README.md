# CSE138_Assignment4

## Acknowledgements
`Oleksiy Omelchenko:`
### Aided by Patrick Redmond
- Patrick spent one hour helping Oleksiy debug a possible race condition causing certain nodes not being able to send GET view requests to other nodes. For example sometimes node1 wasn't able to send to node7 and this error arised: `http://10.10.0.7:8085/key-value-store-view": dial tcp 10.10.0.7:8085: connect: connection refused`. With Patrick's help the error was fixed allowing the group to pass the tests that rely on the looking at the views.

### Helped by Abhay Pande 
- Abhay the day prior to consulting Patrick, helped to test the same issue that Patrick aided in debugging. Although, Oleksiy wasn't able to fix the issue, Abhay provided great feedback by suggesting to increase the time spent `sleeping` and the number of GET requests resent if the prior attempt failed.  

`Zachary Zulanas`
### Helped by Patrick Redmond
- Before implementing any sharding strategy, I went to Patrick's OH to learn more about what approach is best for how we should implement our own sharding strategy. I went in with the idea of implementing consistent hashing, but Patrick told me that would be a bit much for our assignment, and to keep the implementation simple, and then iterate upon it. Because of Patrick I saved a lot of time on implementing the resharding strategy, since consistent hashing would have made our program much more complex.

## Citations
- In the package [hash](https://golang.org/pkg/hash/fnv/), the FNV hash function was used to determine which shard a key will hash into. By using the hash function we learned that this package allows users to use either a `32-bit` or `64-bit` hash function. Also by using the hash function, the group learned that we didn't have to worry about how the hash function operates, only where to apply it. Our group ended up using the `32-bit` hash function to decide which shard a key-value pair will hash to. Some places that use the hash function is the GET, PUT, & DELETE `\key-value-store\key` endpoints. 


- http://www.isthe.com/chongo/tech/comp/fnv/

Information on the FNV Hash function and why to use it. Learned about the FNV hash function is used and it's benefits. Because there's low collision rates with the FNV hash, we decided it would be best so we could get a better spread of keys across shards.

- https://gist.github.com/lenage/e8161cdfc461bf968f8687bbeae51dee

Example code to create our own hashing function using hash/fnv. We used this code to understand how to use the `hash/fnv` library and how to properly hash our keys. Reference for the `HashModN()` function.

- Sometimes the group had to convert strings to integers and vice versa, so we used the [strconv](https://golang.org/pkg/strconv/) package. In particular the `strconv.Atoi` to convert a string to integer and `strconv.Itoa` to convert a integer to a string. Prior to searching up this package, Oleksiy assumed that Golang allowed type-casting between int to string and vice versa. However, some errors arised when attempting this and Oleksiy therefore learned of the existence of the `strconv` package. The `strconv` package was used to convert the `shardCount` environment variable which is a string to an integer in all the shard GET operations and PUT operation for adding a node to a shard.  


## Team Contributions

Oleksiy Omelchenko: Worked on all the GET shard operations and PUT shard operation for adding a node to a shard. He also updated the PUT code from assignment3 for the `\key-value-store\key` endpoint. All the GET shard operation endpoints included `/key-value-store-shard/shard-ids`, `/key-value-store-shard/node-shard-id`, `/key-value-store-shard/shard-id-key-count/:id`, `key-value-store-shard/shard-id-members/:id`, and `key-value-store-shard/shard-id-all_members/:id`. The PUT shard endpoint worked on was `/key-value-store-shard/add-member/:id`.

Zach Zulanas: He implemented the resharding functionality and the hash function that was used for his own code and by the other two team members. The endpoint that he worked on was `/key-value-store-shard/reshard`. Designed the resharding strategy and implemented shard to shard communication to update the KVStore after resharding using `/key-value-store-shard/chunk-r` and `/key-value-store-shard/updatesm` to update the shard members after resharding.

Jackie Garcia: Implemented the key-value store requests for GET and DELETE requests on the `\key-value-store\key` endpoint. 
