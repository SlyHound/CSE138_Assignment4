package utility

import (
	"fmt"
	"strconv"
)

// does the initial spliting process of replica's to shards (Note: this does not include any resharding)
func InitialSharding(s *SharedShardInfo, view *View, shardCount string) {
	Mu.Mutex.Lock()
	count, _ := strconv.Atoi(shardCount)
	numNodes := len(view.PersonalView) / count
	fmt.Println("Check count & numNodes:", count, numNodes)

	for index := 0; index < count; index++ {
		if index == 0 {
			s.ShardMembers[index] = append(s.ShardMembers[index], view.PersonalView[0:numNodes]...)
		} else { // otherwise index == 1
			s.ShardMembers[index] = append(s.ShardMembers[index], view.PersonalView[numNodes:]...)
		}
	}
	Mu.Mutex.Unlock()
}
