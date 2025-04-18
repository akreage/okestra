package activity

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

// Data exchange hub.
// all tasks sends and receives data from this server.

type ActivityStageStatus string

const (
	ActivityStageStatusPending ActivityStageStatus = "pending"
	ActivityStageStatusHalted  ActivityStageStatus = "halted"
	ActivityStageStatusRunning ActivityStageStatus = "running"
	ActivityStageStatusDone    ActivityStageStatus = "done"
)

type ActivityStageStatusUpdateFunc func(status ActivityStageStatus)

// DataExchange is a struct that provides a way for task subscribers to request data.
// Data is cached in activity stage cache until a demand is made for the data.
// The ack is a number that is incremented for every new data processed.
type DataExchange struct {
	MinDemand int
	MaxDemand int
	Ready     bool
	Flush     bool
	TaskId    uuid.UUID
}

type CacheData struct {
	data []*TaskData
	ack  int
}

type ActivityStageInterface interface {
	// registers a task channel.
	// Each task returns a write channel to send data on,
	// and we send a read channel for them to receive data on.
	// Task channels are registered with a taskId
	RegisterTaskChannel(taskId uuid.UUID, taskChan chan<- TaskData) (chan<- TaskData, chan<- DataExchange, error)
	// start an activity stage
	// the initial data is sent out via the channel
	Start()
	// init to initialize the services
	Init(initData []TaskData)
	// check if system is ready to receive requests
	IsReady() bool
	// close an activity stage
	Stop()
}

// TaskChannel implements the TaskChannelInterface
type ActivityStage struct {
	cache                    map[uuid.UUID]*CacheData      // holds the data for each task channel
	writeChan                chan TaskData                 // channel that all task writes will be sent on
	broadcastChan            map[uuid.UUID]chan<- TaskData // array of channels to broadcast data to
	dataExchangeChan         chan DataExchange             // all requests for data by subscribers is sent on this channel
	taskAcks                 map[uuid.UUID]int             // holds the ack for each task
	pendingExchanges         []*DataExchange               // holds the pending exchange requests which are waiting to be fulfilled
	newDataChan              chan bool                     // alerts when new data is available.
	Graph                    *ActivityGraph
	wg                       sync.WaitGroup
	lock                     sync.RWMutex
	ctx                      context.Context
	cancel                   context.CancelFunc
	IncomingDataHandlerReady bool
	OutgoingDataHandlerReady bool
}

func NewActivityStage(graph *ActivityGraph) ActivityStageInterface {
	ctx, cancel := context.WithCancel(context.Background())
	activityStage := &ActivityStage{
		cache:                    make(map[uuid.UUID]*CacheData),
		writeChan:                make(chan TaskData),
		broadcastChan:            make(map[uuid.UUID]chan<- TaskData),
		dataExchangeChan:         make(chan DataExchange),
		taskAcks:                 make(map[uuid.UUID]int),
		newDataChan:              make(chan bool),
		Graph:                    graph,
		ctx:                      ctx,
		cancel:                   cancel,
		IncomingDataHandlerReady: false,
		OutgoingDataHandlerReady: false,
	}

	return activityStage
}

func (as *ActivityStage) Start() {
	// create a goroutine to receive data from the receive chan
	// create a goroutine to receive data exchange requests
	as.handleIncomingData()
	as.handleDataExchangeRequests()
}

func (as *ActivityStage) Init(initData []TaskData) {
	// get the first task
	taskId := as.getFirstTask()
	if taskId == uuid.Nil {
		// TODO: add error to telemetry
		err := ActivityError{
			Op:  "addInitialData",
			Err: errors.New("no initial task id found"),
			Key: "no_task_id_found",
		}
		panic(err)
	}

	if len(initData) == 0 {
		return
	}

	// send the data to the task
	for _, data := range initData {
		as.addDataToCache(taskId, &data)
	}
	as.newDataChan <- true
}

func (as *ActivityStage) RegisterTaskChannel(taskId uuid.UUID, receiveChan chan<- TaskData) (chan<- TaskData, chan<- DataExchange, error) {
	// register the task channel
	as.lock.Lock()
	defer as.lock.Unlock()

	as.broadcastChan[taskId] = receiveChan

	return as.writeChan, as.dataExchangeChan, nil
}

func (as *ActivityStage) Stop() {
	as.cancel()
	close(as.dataExchangeChan)
	close(as.writeChan)

	as.wg.Wait()
}

func (as *ActivityStage) IsReady() bool {
	return as.IncomingDataHandlerReady && as.OutgoingDataHandlerReady
}

func (as *ActivityStage) handleIncomingData() {
	as.wg.Add(1)

	go func() {
		defer as.wg.Done()

		// receive data on
		as.IncomingDataHandlerReady = true
		for {
			select {
			case <-as.ctx.Done():
				return
			case data, ok := <-as.writeChan:
				if !ok {
					// TODO: add telemetry here
					return
				}

				as.lock.Lock()
				// identify the tasks that need to receive the data
				// add the task data to thir cache
				taskIds := as.listTasksThatDataShouldBeSentTo(data)
				for _, taskId := range taskIds {
					as.addDataToCache(taskId, &data)
				}
				// maybe forward data to all pending taskIds
				as.lock.Unlock()
				as.newDataChan <- true
				// TODO: add a way to alert the data exchanger to fetch the new data
			}
		}
	}()
}

func (as *ActivityStage) addDataToCache(taskId uuid.UUID, data *TaskData) {
	// get the last data in the cache
	// update the ack number to its ack number + 1
	cacheData, exists := as.cache[taskId]
	if !exists {
		// create a new cache data
		as.cache[taskId] = &CacheData{
			data: []*TaskData{data},
			ack:  0,
		}
		return
	}

	// update the data in the cache
	cacheData.data = append(cacheData.data, data)
}

func (as *ActivityStage) handleDataExchangeRequests() {
	// data exchange requests are received on this channel.
	//  it is shared by all tasks.

	// if a task requests data, we find the task Id, the data requirements
	// and forward the requested data to the task.
	as.wg.Add(1)

	go func() {
		defer as.wg.Done()

		as.OutgoingDataHandlerReady = true
		for {
			select {
			case <-as.ctx.Done():
				return
			// get all pending exchange requests and try sending them out
			case <-as.newDataChan:
				// we need a read lock
				as.lock.Lock()
				for _, exchangeRequest := range as.pendingExchanges {
					as.sendOutTaskData(exchangeRequest.TaskId, exchangeRequest)
				}
				as.lock.Unlock()

			// receive a data exchange request
			case exchangeRequest := <-as.dataExchangeChan:
				as.lock.Lock()
				// get associated task and check if it has outstanding data
				if err := as.updateTaskAck(exchangeRequest.TaskId, &exchangeRequest); err == nil {
					// try sending out data
					as.sendOutTaskData(exchangeRequest.TaskId, &exchangeRequest)
				}

				as.lock.Unlock()
			}
		}
	}()
}

func (as *ActivityStage) listTasksThatDataShouldBeSentTo(data TaskData) []uuid.UUID {
	// get the task from the graph
	edges := as.Graph.Edges[data.TaskId.String()]
	result := make([]uuid.UUID, 0)

	// get the tasks that should receive the data
	for _, edge := range edges {
		// convert the edge to a uuid
		uuid, err := uuid.Parse(edge)
		if err != nil {
			continue
		}
		result = append(result, uuid)
	}

	return result
}

func (as *ActivityStage) getFirstTask() uuid.UUID {
	// get the first task from the graph
	topoSort, err := as.Graph.TopologicalSort()
	if err != nil {
		// TODO: add error to telemetry
		return uuid.Nil
	}

	// return the first task
	return uuid.MustParse(topoSort[0])
}

func (as *ActivityStage) updateTaskAck(taskID uuid.UUID, exchangeRequest *DataExchange) error {
	// if there is a request to flush, get the ack number and flush to that number
	// then update the ack number to 0
	taskData, exists := as.cache[taskID]

	if !exists || len(taskData.data) == 0 {
		as.updateExchangeRequest(exchangeRequest)
		return &ActivityError{
			Op:  "updateTaskAck",
			Err: errors.New("no data to update ack for"),
			Key: "no_data_to_update_ack_for",
		}
	}

	// for a flush, data is flushed up to the current ack number
	// and the ack number is set to 0
	if exchangeRequest.Flush {
		// flash all the data up to the ack
		taskData.data = taskData.data[taskData.ack:]
		// set the taskId ack to 0
		taskData.ack = 0
	}

	return nil
}

func (as *ActivityStage) sendOutTaskData(taskID uuid.UUID, exchangeRequest *DataExchange) error {
	// get ack for the taskId
	taskData, exists := as.cache[taskID]
	if !exists || len(taskData.data) == 0 {
		return &ActivityError{
			Op:  "sendOutTaskData",
			Err: errors.New("no data to send out"),
			Key: "no_data_to_send_out",
		}
	}

	length := len(taskData.data)
	maxDemand := taskData.ack + exchangeRequest.MaxDemand
	if maxDemand > length {
		maxDemand = length
	}

	// get the data from cache starting from the ackNumber, returning maxDemand number of data
	fetchedData := taskData.data[taskData.ack:maxDemand]

	// ensure the taskData is not less than the minDemand
	// else return
	if len(fetchedData) < exchangeRequest.MinDemand {
		// lets wait for more data to arrive before sending them out
		// save the exchangeRequest for a future send
		as.updateExchangeRequest(exchangeRequest)
		return &ActivityError{
			Op:  "sendOutTaskData",
			Err: errors.New("not enough data to send out"),
			Key: "not_enough_data_to_send_out",
		}
	}

	// send the data to the subscribers
	for _, data := range fetchedData {
		// get the data
		taskDataCopy := *data
		broadcastChan, exists := as.broadcastChan[taskID]
		if !exists {
			// TODO: add error to telemetry
			continue
		}

		broadcastChan <- taskDataCopy
		taskData.ack++
	}

	// delete the exchange request from the list
	as.deleteExchangeRequest(exchangeRequest)
	return nil
}

func (as *ActivityStage) updateExchangeRequest(exchangeRequest *DataExchange) {
	// saves the exchange request for the future.
	// this happens there is not enough data to satisfy the minimum demand requirements
	for index, ex := range as.pendingExchanges {
		if ex.TaskId == exchangeRequest.TaskId {
			// replace the existing request
			as.pendingExchanges[index] = exchangeRequest
			return
		}
	}

	// add the new request
	as.pendingExchanges = append(as.pendingExchanges, exchangeRequest)
}

func (as *ActivityStage) deleteExchangeRequest(exchangeRequest *DataExchange) {
	for i, ex := range as.pendingExchanges {
		if ex.TaskId == exchangeRequest.TaskId {
			as.pendingExchanges[i] = as.pendingExchanges[len(as.pendingExchanges)-1]
			as.pendingExchanges = as.pendingExchanges[:len(as.pendingExchanges)-1]
			break
		}
	}
}
