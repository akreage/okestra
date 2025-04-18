package activity

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type TestTask struct {
	Id              uuid.UUID
	Name            string
	Description     string
	SendDataChan    chan<- TaskData
	ReceiveDataChan chan TaskData
	exchangeChan    chan<- DataExchange
	dataSet         []*TaskData
	wg              sync.WaitGroup
}

func createTask(name string) *TestTask {
	return &TestTask{
		Id:              uuid.New(),
		Name:            name,
		ReceiveDataChan: make(chan TaskData),
		Description:     "This is a test task",
	}
}

func (t *TestTask) createAndRegisterTaskChannek(activityStage ActivityStageInterface) {
	sendChan, exchangeChan, err := activityStage.RegisterTaskChannel(t.Id, t.ReceiveDataChan)
	t.SendDataChan = sendChan
	t.exchangeChan = exchangeChan

	if err != nil {
		panic(err)
	}

	// receive data from the receive channel
	t.wg.Add(1)
	go func() {
		defer t.wg.Done()

		for data := range t.ReceiveDataChan {
			t.dataSet = append(t.dataSet, &data)
		}
	}()
}

func (t *TestTask) close() {
	close(t.ReceiveDataChan)
	t.wg.Wait()
}

func (t *TestTask) sendData(data TaskData) {
	t.SendDataChan <- data
}

func (t *TestTask) requestData(flush bool, minDemand, maxDemand int) {
	t.exchangeChan <- DataExchange{
		TaskId:    t.Id,
		MinDemand: minDemand,
		MaxDemand: maxDemand,
		Flush:     flush,
	}
}

func (t *TestTask) getDataExchange(flush bool, minDemand, maxDemand int) DataExchange {
	return DataExchange{
		TaskId:    t.Id,
		MinDemand: minDemand,
		MaxDemand: maxDemand,
		Flush:     flush,
	}
}

func TestActivityStageInit(t *testing.T) {
	t.Run("ok: data is sent out to the task", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			assert.Equal(t, 0, len(as.cache))
			initialData := createTaskData(uuid.New())
			activityStage.Init([]TaskData{*initialData})

			assert.Equal(t, 1, len(as.cache))
		})
	})
}

func TestSendOutTaskData(t *testing.T) {
	t.Run("ok: data is sent out to the task", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			// init data
			initData := createTaskData(uuid.New())
			activityStage.Init([]TaskData{*initData})

			// assert the task1 dataset is empty
			assert.Equal(t, 0, len(task1.dataSet))

			// send data out to the task
			exchangeRequest := task1.getDataExchange(false, 1, 10)
			err := as.sendOutTaskData(task1.Id, &exchangeRequest)
			assert.Nil(t, err)

			// wait for 200 ms
			time.Sleep(100 * time.Millisecond)

			// check that data was sent to the correct task
			assert.Equal(t, 1, len(task1.dataSet))
			// assert the exchange request list is empty
			assert.Equal(t, 0, len(as.pendingExchanges))
		})
	})

	t.Run("error: no data for the task", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			exchangeRequest := task1.getDataExchange(false, 1, 10)
			err := as.sendOutTaskData(task1.Id, &exchangeRequest)
			assert.Error(t, err)

			// convert error to ActivityError
			activityError, ok := err.(*ActivityError)
			assert.True(t, ok)
			assert.Equal(t, "sendOutTaskData", activityError.Op)
			assert.Equal(t, "no_data_to_send_out", activityError.Key)
			assert.Equal(t, "no data to send out", activityError.Err.Error())
		})
	})

	t.Run("ok: task exists in cache but no data", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			// init data
			activityStage.Init([]TaskData{})

			// send data out to the task
			exchangeRequest := task1.getDataExchange(false, 1, 10)
			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{},
				ack:  0,
			}

			err := as.sendOutTaskData(task1.Id, &exchangeRequest)
			assert.Error(t, err)

			// convert error to ActivityError
			activityError, ok := err.(*ActivityError)
			assert.True(t, ok)
			assert.Equal(t, "sendOutTaskData", activityError.Op)
			assert.Equal(t, "no_data_to_send_out", activityError.Key)
			assert.Equal(t, "no data to send out", activityError.Err.Error())
		})
	})

	t.Run("error: minDemand is not satisfied - demands 2 but only 1 data", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			activityStage.Init([]TaskData{})

			// send data out to the task
			exchangeRequest := task1.getDataExchange(false, 2, 10)

			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{
					createTaskData(uuid.New()),
				},
				ack: 0,
			}

			err := as.sendOutTaskData(task1.Id, &exchangeRequest)
			assert.Error(t, err)

			// convert error to ActivityError
			activityError, ok := err.(*ActivityError)
			assert.True(t, ok)
			assert.Equal(t, "sendOutTaskData", activityError.Op)
			assert.Equal(t, "not_enough_data_to_send_out", activityError.Key)
			assert.Equal(t, "not enough data to send out", activityError.Err.Error())

			// assert no data is received by task1
			assert.Equal(t, 0, len(task1.dataSet))
		})
	})

	t.Run("ok: minDemand is satisfied - demands 2 but gets all data", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			activityStage.Init([]TaskData{})

			// task data
			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
				},
				ack: 0,
			}

			// send data out to the task
			exchangeRequest := task1.getDataExchange(false, 2, 10)
			err := as.sendOutTaskData(task1.Id, &exchangeRequest)
			assert.Nil(t, err)

			// wait for 200 ms
			time.Sleep(100 * time.Millisecond)

			// assert the task1 dataset is empty
			assert.Equal(t, 3, len(task1.dataSet))
			assert.Equal(t, 3, as.cache[task1.Id].ack)
			assert.Equal(t, 0, len(as.pendingExchanges))
		})
	})

	t.Run("ok: more than maxDemand - 6 but max is 5", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			activityStage.Init([]TaskData{})

			// task data
			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
				},
				ack: 0,
			}

			// send data out to the task
			exchangeRequest := task1.getDataExchange(false, 2, 5)
			err := as.sendOutTaskData(task1.Id, &exchangeRequest)
			assert.Nil(t, err)

			// wait for 200 ms
			time.Sleep(100 * time.Millisecond)

			// assert the task1 dataset is empty
			assert.Equal(t, 5, len(task1.dataSet))
			assert.Equal(t, 5, as.cache[task1.Id].ack)
		})
	})

	t.Run("ok: send out data with different task ids", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
				},
				ack: 0,
			}

			// send out request for task 1
			exchangeRequest := task1.getDataExchange(false, 1, 2)
			as.updateTaskAck(task1.Id, &exchangeRequest)
			as.sendOutTaskData(task1.Id, &exchangeRequest)

			// wait for 200 ms
			time.Sleep(100 * time.Millisecond)

			// check that data was sent to the correct task
			assert.Equal(t, 2, len(task1.dataSet))

			// received new data here
			as.addDataToCache(task1.Id, createTaskData(uuid.New()))
			as.addDataToCache(task1.Id, createTaskData(uuid.New()))

			// send out request for task 2
			exchangeRequest2 := task1.getDataExchange(true, 1, 2)
			as.updateTaskAck(task1.Id, &exchangeRequest2)
			as.sendOutTaskData(task1.Id, &exchangeRequest2)

			// wait for 200 ms
			time.Sleep(100 * time.Millisecond)

			// check that data was sent to the correct task
			assert.Equal(t, 4, len(task1.dataSet))
			assert.Equal(t, 0, len(as.pendingExchanges))
			assert.Equal(t, 3, len(as.cache[task1.Id].data))
			assert.Equal(t, 2, as.cache[task1.Id].ack)
		})
	})
}

func TestUpdateTaskAck(t *testing.T) {
	t.Run("ok: task ack is updated", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			// update task ack by one
			task1 := tasks[0]
			exchangeRequest := task1.getDataExchange(false, 1, 2)
			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
				},
				ack: 0,
			}

			// already handled first task
			err := as.updateTaskAck(task1.Id, &exchangeRequest)
			assert.Nil(t, err)

			// ack is updated only when data is sent out
			assert.Equal(t, 0, as.cache[task1.Id].ack)
			assert.Equal(t, 3, len(as.cache[task1.Id].data))
		})
	})

	t.Run("ok: ack set flush to true", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			exchangeRequest := task1.getDataExchange(true, 1, 2)
			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
					createTaskData(uuid.New()),
				},
				ack: 2,
			}

			// already handled first task
			err := as.updateTaskAck(task1.Id, &exchangeRequest)
			assert.Nil(t, err)

			// check that the task ack is updated
			assert.Equal(t, 0, as.cache[task1.Id].ack)
			assert.Equal(t, 1, len(as.cache[task1.Id].data))
		})
	})

	t.Run("error: requested cache not existing", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			exchangeRequest := task1.getDataExchange(false, 1, 2)

			err := as.updateTaskAck(task1.Id, &exchangeRequest)
			assert.NotNil(t, err)

			// convert error to ActivityError
			activityError, ok := err.(*ActivityError)
			assert.True(t, ok)
			assert.Equal(t, "updateTaskAck", activityError.Op)
			assert.Equal(t, "no_data_to_update_ack_for", activityError.Key)
			assert.Equal(t, "no data to update ack for", activityError.Err.Error())
		})
	})

	t.Run("error: cache for task is empty", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			// add task to cache but no data
			task1 := tasks[0]
			exchangeRequest := task1.getDataExchange(true, 1, 2)
			as.cache[task1.Id] = &CacheData{
				data: []*TaskData{},
				ack:  0,
			}

			err := as.updateTaskAck(task1.Id, &exchangeRequest)
			assert.NotNil(t, err)

			// convert error to ActivityError
			activityError, ok := err.(*ActivityError)
			assert.True(t, ok)
			assert.Equal(t, "updateTaskAck", activityError.Op)
			assert.Equal(t, "no_data_to_update_ack_for", activityError.Key)
			assert.Equal(t, "no data to update ack for", activityError.Err.Error())
		})
	})
}

// lets run some tests
func TestActivityStageWithFourTasks(t *testing.T) {
	t.Run("ok: init data is sent out before task requests", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			task2 := tasks[1]
			task3 := tasks[2]
			task4 := tasks[3]

			// send data out initial data
			initTask := createTaskData(uuid.New())

			// init the activity stage
			activityStage.Init([]TaskData{*initTask})

			// request data for all tasks
			requestDataForTasks(tasks, 1, 10)

			// wait for 100 ms
			time.Sleep(100 * time.Millisecond)

			// assert task 1 received the data
			assert.Equal(t, 1, len(task1.dataSet))
			assert.Equal(t, 0, len(task2.dataSet))
			assert.Equal(t, 0, len(task3.dataSet))
			assert.Equal(t, 0, len(task4.dataSet))
		})
	})

	t.Run("ok: init data is sent out after task requests", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			task2 := tasks[1]
			task3 := tasks[2]
			task4 := tasks[3]

			// request data for all tasks
			requestDataForTasks(tasks, 1, 10)

			// wait for 100 ms
			time.Sleep(100 * time.Millisecond)

			initTask := createTaskData(uuid.New())
			activityStage.Init([]TaskData{*initTask})

			// wait for 100 ms
			time.Sleep(100 * time.Millisecond)

			// assert task 1 received the data
			assert.Equal(t, 1, len(task1.dataSet))
			assert.Equal(t, 0, len(task2.dataSet))
			assert.Equal(t, 0, len(task3.dataSet))
			assert.Equal(t, 0, len(task4.dataSet))

			// assert pending exchanges are is 3
			assert.Equal(t, 3, len(as.pendingExchanges))
		})
	})

	t.Run("ok: init data is sent then another sent from task 1", func(t *testing.T) {
		startActivityAndReadyFunc(func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask) {
			defer stopService(activityStage, tasks)

			task1 := tasks[0]
			task2 := tasks[1]
			task3 := tasks[2]
			task4 := tasks[3]

			// request data for all tasks
			requestDataForTasks(tasks, 1, 10)

			// wait for 100 ms
			time.Sleep(100 * time.Millisecond)

			initTask := createTaskData(uuid.New())
			activityStage.Init([]TaskData{*initTask})

			// wait for 100 ms
			time.Sleep(100 * time.Millisecond)

			task1.sendData(*createTaskData(task1.Id))
			task1.sendData(*createTaskData(task1.Id))

			// wait for 100ms
			time.Sleep(100 * time.Millisecond)

			// assert task 1 received the data
			assert.Equal(t, 1, len(task1.dataSet))
			assert.Equal(t, 2, len(task2.dataSet))
			assert.Equal(t, 0, len(task3.dataSet))
			assert.Equal(t, 0, len(task4.dataSet))

			// send data out from task 2
			task2.sendData(*createTaskData(task2.Id))
			task2.sendData(*createTaskData(task2.Id))
			task2.sendData(*createTaskData(task2.Id))

			// wait for 100ms
			time.Sleep(100 * time.Millisecond)

			// assert task 1 received the data
			assert.Equal(t, 1, len(task1.dataSet))
			assert.Equal(t, 2, len(task2.dataSet))
			assert.Equal(t, 2, len(task3.dataSet))
			assert.Equal(t, 0, len(task4.dataSet))

			// send data out from task 3
			task3.sendData(*createTaskData(task3.Id))
			task3.sendData(*createTaskData(task3.Id))

			// wait for 100ms
			time.Sleep(100 * time.Millisecond)

			// assert task 1 received the data
			assert.Equal(t, 1, len(task1.dataSet))
			assert.Equal(t, 2, len(task2.dataSet))
			assert.Equal(t, 2, len(task3.dataSet))
			assert.Equal(t, 2, len(task4.dataSet))

			// send data out from task 4
			// no action should be taken
			task4.sendData(*createTaskData(task4.Id))
			task4.sendData(*createTaskData(task4.Id))

			// wait for 100ms
			time.Sleep(100 * time.Millisecond)

			// assert task 1 received the data
			assert.Equal(t, 1, len(task1.dataSet))
			assert.Equal(t, 2, len(task2.dataSet))
			assert.Equal(t, 2, len(task3.dataSet))
			assert.Equal(t, 2, len(task4.dataSet))

			// send data from task 1
			task1.sendData(*createTaskData(task1.Id))
			task1.sendData(*createTaskData(task1.Id))

			// wait for 100ms
			time.Sleep(100 * time.Millisecond)

			// at this point, pending exchanges should be empty
			assert.Equal(t, 0, len(as.pendingExchanges))

			// send exchange request out for all tasks
			requestDataForTasks(tasks, 1, 10)

			// wait for 100ms
			time.Sleep(200 * time.Millisecond)

			// assert task 1 received the data
			assert.Equal(t, 1, len(task1.dataSet))
			assert.Equal(t, 4, len(task2.dataSet))
			assert.Equal(t, 3, len(task3.dataSet))
			assert.Equal(t, 2, len(task4.dataSet))
		})
	})
}

func createTaskData(taskId uuid.UUID) *TaskData {
	return &TaskData{
		TaskId: taskId,
		Data: []byte(fmt.Sprintf("random-test-data-%s",
			taskId.String())),
	}
}

func createTasks(activityStage ActivityStageInterface, graph *ActivityGraph) []*TestTask {
	// tasks
	task1 := createTask("task1")
	task2 := createTask("task2")
	task3 := createTask("task3")
	task4 := createTask("task4")

	// create a graph
	graph.AddNode(task1.Id.String())
	graph.AddNode(task2.Id.String())
	graph.AddNode(task3.Id.String())
	graph.AddNode(task4.Id.String())
	graph.AddEdge(task1.Id.String(), task2.Id.String())
	graph.AddEdge(task2.Id.String(), task3.Id.String())
	graph.AddEdge(task3.Id.String(), task4.Id.String())

	// create and register task channels
	task1.createAndRegisterTaskChannek(activityStage)
	task2.createAndRegisterTaskChannek(activityStage)
	task3.createAndRegisterTaskChannek(activityStage)
	task4.createAndRegisterTaskChannek(activityStage)

	return []*TestTask{task1, task2, task3, task4}
}

func requestDataForTasks(tasks []*TestTask, minDemand, maxDemand int) {
	// for each task, request data
	for _, task := range tasks {
		task.requestData(false, minDemand, maxDemand)
	}
}

func startActivityAndReadyFunc(fn func(as *ActivityStage, activityStage ActivityStageInterface, tasks []*TestTask)) {
	// create and start an activity stage
	graph := NewActivityGraph()
	activityStage := NewActivityStage(graph)
	tasks := createTasks(activityStage, graph)

	activityStage.Start()

	// wait for the activity stage to be ready
	for !activityStage.IsReady() {
		time.Sleep(50 * time.Millisecond)
	}

	as := activityStage.(*ActivityStage)

	fn(as, activityStage, tasks)
}

func stopService(activityStage ActivityStageInterface, tasks []*TestTask) {
	for _, task := range tasks {
		task.close()
	}

	activityStage.Stop()
}
