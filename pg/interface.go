package pg

// signal definition
// signal is data sent between owner and members of a pg group
// there are two types of signals:
// - request: request from owner to member
// - response: response from member to owner
type PGSignalSource string
type PGSignalKind string
type PGSignalSendMode string
type PGSignalKillReason string

const (
	// signal ownership
	PGSignalSourceOwner  PGSignalSource = "owner"  // owner of the group
	PGSignalSourceMember PGSignalSource = "member" // member of the group

	// signal kind
	PGSignalCreateKind PGSignalKind = "create" // add a new member to the group
	PGSignalTypePanic  PGSignalKind = "panic"  // panic signal
	PGSignalTypeNormal PGSignalKind = "normal" // normal signal

	// signal send mode
	PGSignalSendModeBroadcast PGSignalSendMode = "broadcast" // send to all members
	PGSignalSendModeUnicast   PGSignalSendMode = "unicast"   // send to a specific member

	// kill signal reason
	PGSignalKillReasonLeave        PGSignalKillReason = "leave"         // member has completed a task and is leaving the group
	PGSignalKillReasonFatalError   PGSignalKillReason = "fatal_error"   // member has abruptly left the group due to an error
	PGSignalKillReasonMemberKilled PGSignalKillReason = "member_killed" // member has been killed by the owner
)

type PGSignal struct {
	// who is sending the signal
	Source PGSignalSource
	// what kind of signal is being sent
	Signal PGSignalKind
	// how should the signal be sent
	// broadcast: send to all members
	// unicast: send to a specific member
	// only owner can send a broadcast signal
	Mode PGSignalSendMode
	// set for Kill signal kind
	Reason PGSignalKillReason
	// set in case of a unicast signal
	// from owner to member
	RecipientKey string
	// source key
	SourceKey string
}

// pg error
type PGError struct {
	Op  string // the operation that caused the error
	Err error  // the original error
	Key string // related error key
}

// channel group interface

// PGGroupOwner is the owner of a pg group
type PGGroupOwner[T any] interface {
	Key() string
	HandleSignal(source string, signal PGSignal) error
	IsLocal() bool
	NodeName() string
}

// PGMember is a member of a pg group
type PGMember[K any] interface {
	Key() string
	// TODO: fix signal type
	HandleSignal(source string, signal PGSignal) error
}

// ChannelGroup allows to register channels and manage global discovery of parent and children structs/channels
type PGInterface[T PGGroupOwner[T], K PGMember[K]] interface {
	RegisterGroup(owner T) error
	RemoveGroup(owner T) error
	GroupOwner(key string) (T, error)
	Members(key string) (*[]K, error)
	FindOwner(key string) (T, error)
	LocalGroups() (*[]T, error)
	Join(key string, member K) error
	Leave(key string, member string, reason PGSignal) error
	Monitor(owner T, member string) error
	Unmonitor(owner T, member string) error
}
