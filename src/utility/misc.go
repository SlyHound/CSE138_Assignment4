package utility

import (
	"fmt"
	"log"
	"strconv"
)

// does the initial spliting process of replica's to shards (Note: this does not include any resharding)
// TEMPORARY SOLUTION
func InitialSharding(s *SharedShardInfo, view *View, shardCount string) {
	// Mu.Mutex.Lock()
	intShardCount, _ := strconv.Atoi(shardCount)
	if intShardCount <= 0 {
		log.Fatal("ShardCount can't be 0 or negative!")
	}

	numNodes := len(view.PersonalView) / intShardCount
	fmt.Println("Check count & numNodes:", intShardCount, numNodes)

	if numNodes < 2 {
		log.Fatal("Each shard must be able to accommodate least 2 nodes!")
	}

	// as we iterate the for loop, need to know which nodes belong in which shard
	// so the upper/lower bounds tell us which elements in the bound, go where
	lowerBound := 0
	upperBound := numNodes

	for index := 0; index < intShardCount; index++ {
		if index == intShardCount-1 {
			upperBound = len(view.PersonalView)
		}
		s.ShardMembers[index] = append(s.ShardMembers[index], view.PersonalView[lowerBound:upperBound]...)
		lowerBound += numNodes
		upperBound += numNodes
	}
	// Mu.Mutex.Unlock()
}
