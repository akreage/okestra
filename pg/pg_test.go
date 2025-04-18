package pg_test

import (
	"testing"

	"github.com/acreage/okestra/pg"
	"github.com/stretchr/testify/assert"
)

// Mock implementations for testing
type MockOwner struct {
	key     string
	isLocal bool
	signals []pg.PGSignal
}

func (m *MockOwner) Key() string   { return m.key }
func (m *MockOwner) IsLocal() bool { return m.isLocal }
func (m *MockOwner) HandleSignal(memberId string, signal pg.PGSignal) error {
	m.signals = append(m.signals, signal)
	return nil
}
func (m *MockOwner) NodeName() string { return m.key }

type MockMember struct {
	key     string
	signals []pg.PGSignal
}

func (m *MockMember) Key() string { return m.key }
func (m *MockMember) HandleSignal(source string, signal pg.PGSignal) error {
	m.signals = append(m.signals, signal)
	return nil
}

func TestProcessGroup(t *testing.T) {
	t.Run("RegisterGroup", func(t *testing.T) {
		pg := pg.NewProcessGroup[*MockOwner, *MockMember]()
		owner := &MockOwner{key: "owner1", isLocal: true}

		err := pg.RegisterGroup(owner)

		// Verify owner exists
		assert.NoError(t, err)
		retrievedOwner, err := pg.GroupOwner("owner1")
		assert.NoError(t, err)
		assert.Equal(t, owner.Key(), retrievedOwner.Key())
	})

	t.Run("JoinGroup", func(t *testing.T) {
		processGroup := pg.NewProcessGroup[*MockOwner, *MockMember]()
		owner := &MockOwner{key: "owner1", isLocal: true}
		member := &MockMember{key: "member1"}

		// Register group first
		err := processGroup.RegisterGroup(owner)
		assert.NoError(t, err, "Failed to register group")

		// Join group
		err = processGroup.Join(owner.Key(), member)
		assert.NoError(t, err, "Failed to join group")

		// Verify member exists in group
		members, err := processGroup.Members(owner.Key())
		assert.NoError(t, err, "Failed to get members")
		assert.Equal(t, 1, len(*members), "Expected 1 member, got %d", len(*members))
		assert.Equal(t, member.Key(), (*members)[0].Key(), "Expected member key %s, got %s", member.Key(), (*members)[0].Key())

		// verify owner has 1 signal and kind is create
		assert.Equal(t, 1, len(owner.signals), "Expected 1 signal, got %d", len(owner.signals))
		assert.Equal(t, pg.PGSignalCreateKind, owner.signals[0].Signal, "Expected signal kind %s, got %s", pg.PGSignalCreateKind, owner.signals[0].Signal)
	})

	t.Run("LeaveGroup - member is unmonitored", func(t *testing.T) {
		errReason := pg.PGSignal{
			Source: pg.PGSignalSourceMember,
			Signal: pg.PGSignalTypeNormal,
			Mode:   pg.PGSignalSendModeUnicast,
			Reason: pg.PGSignalKillReasonLeave,
		}
		processGroup := pg.NewProcessGroup[*MockOwner, *MockMember]()
		owner := &MockOwner{key: "owner1", isLocal: true}
		member := &MockMember{key: "member1"}

		// Setup group
		err := processGroup.RegisterGroup(owner)
		assert.NoError(t, err, "Failed to register group")
		err = processGroup.Join(owner.Key(), member)
		assert.NoError(t, err, "Failed to join group")

		errReason.RecipientKey = member.Key()

		// Leave group
		// member is unmonitored, no action required from owner
		err = processGroup.Leave(owner.Key(), member.Key(), errReason)
		assert.NoError(t, err, "Failed to leave group")

		// Verify member is removed
		members, err := processGroup.Members(owner.Key())
		assert.NoError(t, err, "Failed to get members")
		assert.Equal(t, 0, len(*members), "Expected 0 members, got %d", len(*members))

		// verify owner has 1 signal and kind is create
		// since the process is unmonitored, owner will not receive a kill signal
		assert.Equal(t, 1, len(owner.signals), "Expected 1 signal, got %d", len(owner.signals))
		assert.Equal(t, pg.PGSignalCreateKind, owner.signals[0].Signal, "Expected signal kind %s, got %s", pg.PGSignalCreateKind, owner.signals[0].Signal)
	})

	t.Run("LeaveGroup - member is monitored", func(t *testing.T) {
		errReason := pg.PGSignal{
			Source: pg.PGSignalSourceMember,
			Signal: pg.PGSignalTypeNormal,
			Mode:   pg.PGSignalSendModeUnicast,
			Reason: pg.PGSignalKillReasonLeave,
		}
		processGroup := pg.NewProcessGroup[*MockOwner, *MockMember]()
		owner := &MockOwner{key: "owner1", isLocal: true}
		member := &MockMember{key: "member1"}

		// Setup group
		err := processGroup.RegisterGroup(owner)
		assert.NoError(t, err, "Failed to register group")
		err = processGroup.Join(owner.Key(), member)
		assert.NoError(t, err, "Failed to join group")
		err = processGroup.Monitor(owner, member.Key())
		assert.NoError(t, err, "Failed to monitor member")

		errReason.RecipientKey = member.Key()

		// Leave group
		// member is unmonitored, no action required from owner
		err = processGroup.Leave(owner.Key(), member.Key(), errReason)
		assert.NoError(t, err, "Failed to leave group")

		// Verify member is removed
		members, err := processGroup.Members(owner.Key())
		assert.NoError(t, err, "Failed to get members")
		assert.Equal(t, 0, len(*members), "Expected 0 members, got %d", len(*members))

		// verify owner has 2 signal and kind is create
		// since the process is unmonitored, owner will not receive a kill signal
		assert.Equal(t, 2, len(owner.signals), "Expected 2 signal, got %d", len(owner.signals))
		assert.Equal(t, pg.PGSignalCreateKind, owner.signals[0].Signal, "Expected signal kind %s, got %s", pg.PGSignalCreateKind, owner.signals[0].Signal)
		assert.Equal(t, pg.PGSignalTypeNormal, owner.signals[1].Signal, "Expected signal kind %s, got %s", pg.PGSignalTypeNormal, owner.signals[1].Signal)
	})

	t.Run("RemoveGroup", func(t *testing.T) {
		processGroup := pg.NewProcessGroup[*MockOwner, *MockMember]()
		owner := &MockOwner{key: "owner1", isLocal: true}
		member := &MockMember{key: "member1"}

		// Setup
		err := processGroup.RegisterGroup(owner)
		assert.NoError(t, err, "Failed to register group")
		err = processGroup.Join(owner.Key(), member)
		assert.NoError(t, err, "Failed to join group")

		// Remove group
		err = processGroup.RemoveGroup(owner)
		assert.NoError(t, err, "Failed to remove group")

		// Verify group is removed
		_, err = processGroup.GroupOwner(owner.Key())
		assert.Error(t, err, "Expected error when retrieving removed owner")

		// Verify member is removed
		members, err := processGroup.Members(owner.Key())
		assert.Error(t, err, "Failed to get members")
		assert.Nil(t, members, "Expected nil members, got %v", members)
	})
}

// func TestPGError(t *testing.T) {
// err := pg.Error("test", nil, "key1")
// expected := "pg error: operation=test key=key1: <nil>"
//
//	if err.Error() != expected {
//		t.Errorf("Expected error message '%s', got '%s'", expected, err.Error())
//	}
// }
