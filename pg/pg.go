package pg

import (
	"fmt"
	"slices"
	"sync"
)

// set pg interface

type ProcessGroup[T PGGroupOwner[T], K PGMember[K]] struct {
	// map of owners
	owners map[string]T
	// map of members
	members map[string]K
	// map of owner to members
	group map[string][]string
	// map of monitors
	monitors map[string][]string
	// add lock
	lock sync.RWMutex
}

// create a new process group
func NewProcessGroup[T PGGroupOwner[T], K PGMember[K]]() PGInterface[T, K] {
	return &ProcessGroup[T, K]{
		owners:   make(map[string]T),
		members:  make(map[string]K),
		group:    make(map[string][]string),
		monitors: make(map[string][]string),
		lock:     sync.RWMutex{},
	}
}

// register a new group
func (pg *ProcessGroup[T, K]) RegisterGroup(owner T) error {
	// group can registered as local or remote
	// local: the group is registered on the local machine
	// remote: the group is registered on a remote machine
	pg.lock.Lock()
	defer pg.lock.Unlock()
	pg.owners[owner.Key()] = owner
	pg.group[owner.Key()] = []string{}
	pg.monitors[owner.Key()] = []string{}
	return nil
}

// find and return an owner
func (pg *ProcessGroup[T, K]) FindOwner(key string) (T, error) {
	pg.lock.RLock()
	defer pg.lock.RUnlock()
	owner, ok := pg.owners[key]
	if !ok {
		err := Error("FindOwner", fmt.Errorf("owner not found"), key)
		return owner, err
	}

	return owner, nil
}

// remove a group
func (pg *ProcessGroup[T, K]) RemoveGroup(owner T) error {
	// get members,
	// remove the owner from the group
	// remove the owner from the owners map
	// remove the owner from the monitors map

	// lock for update
	pg.lock.Lock()
	defer pg.lock.Unlock()
	memberIds, ok := pg.group[owner.Key()]
	if !ok {
		return Error("RemoveGroup", fmt.Errorf("members not found"), owner.Key())
	}

	// get membets
	members := make([]K, len(memberIds))
	for i, memberId := range memberIds {
		member, ok := pg.members[memberId]
		if ok {
			// if member exists add to the list of members
			members[i] = member
		}
	}

	delete(pg.owners, owner.Key())
	delete(pg.group, owner.Key())
	delete(pg.monitors, owner.Key())

	// publish a member kill signal to all members
	signal := PGSignal{
		Source: PGSignalSourceOwner,
		Signal: PGSignalTypeNormal,
		Mode:   PGSignalSendModeBroadcast,
		Reason: PGSignalKillReasonMemberKilled,
	}

	// send signal to all members
	for _, member := range members {
		member.HandleSignal(owner.Key(), signal)
	}
	return nil
}

// get the owner of a group
func (pg *ProcessGroup[T, K]) GroupOwner(key string) (T, error) {
	pg.lock.RLock()
	defer pg.lock.RUnlock()
	owner, ok := pg.owners[key]
	if !ok {
		var zero T
		return zero, Error("GroupOwner", fmt.Errorf("owner not found"), key)
	}
	return owner, nil
}

// get the members of a group
func (pg *ProcessGroup[T, K]) Members(key string) (*[]K, error) {
	pg.lock.RLock()
	defer pg.lock.RUnlock()
	memberIds, ok := pg.group[key]
	if !ok {
		return nil, Error("Members", fmt.Errorf("members not found"), key)
	}
	members := make([]K, len(memberIds))
	for i, memberId := range memberIds {
		member, ok := pg.members[memberId]
		if ok {
			// if member exists add to the list of members
			members[i] = member
		}
	}
	return &members, nil
}

// get the local groups
func (pg *ProcessGroup[T, K]) LocalGroups() (*[]T, error) {
	pg.lock.RLock()
	defer pg.lock.RUnlock()
	groups := make([]T, 0)
	for _, owner := range pg.owners {
		if owner.IsLocal() {
			groups = append(groups, owner)
		}
	}
	return &groups, nil
}

// join a group
func (pg *ProcessGroup[T, K]) Join(key string, member K) error {
	pg.lock.Lock()
	defer pg.lock.Unlock()
	owner, ok := pg.owners[key]
	if !ok {
		return Error("Join", fmt.Errorf("owner not found"), key)
	}

	// avoid duplicates
	if slices.Contains(pg.group[key], member.Key()) {
		return nil
	}

	pg.group[key] = append(pg.group[key], member.Key())
	pg.members[member.Key()] = member

	// check if the member is already in the group
	// alert owner of member join
	// run this in a go routine
	go func() {
		owner.HandleSignal(member.Key(), PGSignal{
			Source:       PGSignalSourceMember,
			Signal:       PGSignalCreateKind,
			Mode:         PGSignalSendModeUnicast,
			RecipientKey: owner.Key(),
			SourceKey:    member.Key(),
		})
	}()

	return nil
}

// leave a group
func (pg *ProcessGroup[T, K]) Leave(ownerId string, memberId string, reason PGSignal) error {
	pg.lock.Lock()
	defer pg.lock.Unlock()
	// get the owner
	// check if the owner monitors this member
	monitoredList, ok := pg.monitors[ownerId]
	if !ok {
		return Error("Leave", fmt.Errorf("owner not found"), ownerId)
	}

	alertOwner := false
	// check if the member is in the monitored list
	for _, monitored := range monitoredList {
		if monitored == memberId {
			alertOwner = true
			break
		}
	}

	// lock and update
	// delete from group
	_, ok = pg.group[ownerId]
	if ok {
		pg.group[ownerId] = slices.DeleteFunc(pg.group[ownerId], func(id string) bool {
			return id == memberId
		})
	}

	// delete from members
	delete(pg.members, memberId)

	// delete from monitors
	_, ok = pg.monitors[ownerId]
	if ok {
		pg.monitors[ownerId] = slices.DeleteFunc(pg.monitors[ownerId], func(id string) bool {
			return id == memberId
		})
	}

	// alert owner of member leave
	if alertOwner {
		owner := pg.owners[ownerId]
		go owner.HandleSignal(memberId, reason)
	}

	return nil
}

// monitor a group
func (pg *ProcessGroup[T, K]) Monitor(owner T, member string) error {
	pg.lock.Lock()
	defer pg.lock.Unlock()
	// avoid duplicates
	if !(slices.Contains(pg.monitors[owner.Key()], member)) {
		pg.monitors[owner.Key()] = append(pg.monitors[owner.Key()], member)
	}
	return nil
}

// unmonitor a group
func (pg *ProcessGroup[T, K]) Unmonitor(owner T, member string) error {
	pg.lock.Lock()
	defer pg.lock.Unlock()
	pg.monitors[owner.Key()] = slices.DeleteFunc(pg.monitors[owner.Key()], func(id string) bool {
		return id == member
	})
	return nil
}

// pg error
func Error(op string, err error, key string) error {
	return &PGError{
		Op:  op,
		Err: err,
		Key: key,
	}
}

// Add this method to implement the error interface
func (e *PGError) Error() string {
	return fmt.Sprintf("pg error: operation=%s key=%s: %v", e.Op, e.Key, e.Err)
}
