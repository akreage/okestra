package mocks

import (
	"github.com/acreage/okestra/pg"
	"github.com/acreage/okestra/supervisor"
	"github.com/stretchr/testify/mock"
)

type PGMock struct {
	mock.Mock
}

func (pg *PGMock) RegisterGroup(owner supervisor.SupervisorInterface) error {
	args := pg.Called(owner)
	return args.Error(0)
}

func (pg *PGMock) RemoveGroup(owner supervisor.SupervisorInterface) error {
	args := pg.Called(owner)
	return args.Error(0)
}

func (pg *PGMock) GroupOwner(key string) (supervisor.SupervisorInterface, error) {
	args := pg.Called(key)
	return args.Get(0).(supervisor.SupervisorInterface), args.Error(1)
}

func (pg *PGMock) FindOwner(key string) (supervisor.SupervisorInterface, error) {
	args := pg.Called(key)
	return args.Get(0).(supervisor.SupervisorInterface), args.Error(1)
}

func (pg *PGMock) Members(key string) (*[]supervisor.SupervisableInterface, error) {
	args := pg.Called(key)
	return args.Get(0).(*[]supervisor.SupervisableInterface), args.Error(1)
}

func (pg *PGMock) LocalGroups() (*[]supervisor.SupervisorInterface, error) {
	args := pg.Called()
	return args.Get(0).(*[]supervisor.SupervisorInterface), args.Error(1)
}

func (pg *PGMock) Join(key string, member supervisor.SupervisableInterface) error {
	args := pg.Called(key, member)
	return args.Error(0)
}

func (pg *PGMock) Leave(key string, member string, reason pg.PGSignal) error {
	args := pg.Called(key, member, reason)
	return args.Error(0)
}

func (pg *PGMock) Monitor(key supervisor.SupervisorInterface, member string) error {
	args := pg.Called(key, member)
	return args.Error(0)
}

func (pg *PGMock) Unmonitor(key supervisor.SupervisorInterface, member string) error {
	args := pg.Called(key, member)
	return args.Error(0)
}
