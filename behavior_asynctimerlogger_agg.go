// Copyright (c) 2012 - Cloud Instruments Co., Ltd.
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
// 1. Redistributions of source code must retain the above copyright notice, this
//    list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright notice,
//    this list of conditions and the following disclaimer in the documentation
//    and/or other materials provided with the distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
// ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package seelog

import (
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type logItem struct {
	AccountID       string  `json:"accountID"`
	ClusterName     string  `json:"clusterName"`
	EALatency       float64 `json:"eaLatency"`
	HostName        string  `json:"hostName"`
	Ip              string  `json:"ip"`
	Latency         float64 `json:"latency"`
	MetaLatency     float64 `json:"metaLatency"`
	Method          string  `json:"method"`
	Mode            string  `json:"mode"`
	Module          string  `json:"module"`
	Operation       string  `json:"operation"`
	Path            string  `json:"path"`
	Region          string  `json:"region"`
	RequestID       string  `json:"requestID"`
	Runtime         string  `json:"runtime"`
	ScheduleLatency float64 `json:"scheduleLatency"`
	ServiceLatency  float64 `json:"serviceLatency"`
	ServiceName     string  `json:"serviceName"`
	StartTime       int64   `json:"startTime"`
	Status          int32   `json:"status"`
	UserAgent       string  `json:"userAgent"`
	FunctionName    string  `json:"functionName"`
	IdleLatency     float64 `json:"idleLatency"`
}

type items struct {
	AggregationIl map[string]interface{}
	Total         int
}

type aggression struct {
	index string
	keys  map[string]string
}

func (it items) String() string {
	var msg string
	for _, v := range it.AggregationIl {
		by, _ := json.Marshal(v)
		msg += string(by) + "\n"
	}
	return msg
}

type msgQueueMetric struct {
	level   LogLevel
	context LogContextInterface
	message interface{}
}

// asyncTimerLogger represents asynchronous logger which processes the log queue each
// 'duration' nanoseconds
type asyncTimerLoggerAgg struct {
	commonLogger
	agg              *aggression
	msgQueue         *list.List
	queueHasElements *sync.Cond
	interval         time.Duration
}

// NewAsyncLoopLogger creates a new asynchronous loop logger
func NewAsyncTimerLoggerAgg(config *logConfig, interval time.Duration,agginput *aggression) (*asyncTimerLoggerAgg, error) {

	if interval <= 0 {
		return nil, errors.New("async logger interval should be > 0")
	}

	asnTimerLogger := new(asyncTimerLoggerAgg)

	asnTimerLogger.msgQueue = list.New()
	asnTimerLogger.queueHasElements = sync.NewCond(new(sync.Mutex))

	asnTimerLogger.commonLogger = *newCommonLogger(config, asnTimerLogger)

	asnTimerLogger.interval = interval
	asnTimerLogger.agg = agginput

	go asnTimerLogger.processQueues()

	return asnTimerLogger, nil
}

func (asnTimerLogger *asyncTimerLoggerAgg) processItems() (closed bool) {
	asnTimerLogger.queueHasElements.L.Lock()
	defer asnTimerLogger.queueHasElements.L.Unlock()

	for asnTimerLogger.msgQueue.Len() == 0 && !asnTimerLogger.Closed() {
		asnTimerLogger.queueHasElements.Wait()
	}

	if asnTimerLogger.Closed() {
		return true
	}

	asnTimerLogger.processQueueElements()
	return false
}

func (asnTimerLogger *asyncTimerLoggerAgg) Infof(format string, params ...interface{}) {
	context, _ := specifyContext(0, asnTimerLogger.customContext)
	asnTimerLogger.addMsgToQueue(InfoLvl, context, params[0])
}
/*
func (asnTimerLogger *asyncTimerLoggerAgg) processQueueElements() {
	var itemArgs items
	itemArgs.Total = asnTimerLogger.msgQueue.Len()
	if asnTimerLogger.msgQueue.Len() > 0 {
		for {
			backElement := asnTimerLogger.msgQueue.Front()
			if backElement == nil {
				break
			}
			it := backElement.Value.(map[string]interface{})
			ftName := it["FunctionName"].(string)
			aID := it["AccountID"].(string)
			key := ftName + "-" + aID
			if v, ok := itemArgs.AggregationIl[key]; ok{
				v.Count += 1
				v.FunctionName = ftName
				v.Latency += it["Latency"].(float64)
			} else {
				nv := &logAggregation{}
				v.FunctionName = ftName
				nv.Count = 1
				nv.Latency = it["Latency"].(float64)
				itemArgs.AggregationIl[key] = nv
			}
			asnTimerLogger.msgQueue.Remove(backElement)
		}
	}
	asnTimerLogger.processLogMsg(InfoLvl, itemArgs, nil)
}
*/

func (asnTimerLogger *asyncTimerLoggerAgg) processQueueElements() {
	var itemArgs items
	var cx LogContextInterface
	itemArgs.AggregationIl = make(map[string]interface{})
	itemArgs.Total = asnTimerLogger.msgQueue.Len()
	if asnTimerLogger.msgQueue.Len() > 0 {
		for {
			backElement := asnTimerLogger.msgQueue.Front()
			if backElement == nil {
				break
			}
			var funcLog map[string]interface{} //&logItem{}
			msg, _ := backElement.Value.(msgQueueMetric)
			//json.Unmarshal([]byte(msg.message.String()), &funcLog)
			funcLog = msg.message.(map[string]interface{})

			if _, ok := funcLog[asnTimerLogger.agg.index]; !ok {
				cx = msg.context
				//asnTimerLogger.processLogMsg(InfoLvl, msg.message, cx)
				asnTimerLogger.msgQueue.Remove(backElement)
				return
			} else {
				ftName := funcLog[asnTimerLogger.agg.index].(string)
				cx = msg.context
				if ag, ok := itemArgs.AggregationIl[ftName]; ok {
					ag := ag.(map[string]interface{})
					for k, v := range asnTimerLogger.agg.keys {
						if v == "string" {
							continue
						} else{
							ag[k] = ag[k].(float64) + funcLog[k].(float64)
						}
					}
					ag["count"] = ag["count"].(int) + 1
				} else {
					nv := make(map[string]interface{})
					for k := range asnTimerLogger.agg.keys {
						nv[k]  = funcLog[k]
					}
					nv["count"] = 1
					itemArgs.AggregationIl[ftName] = nv
				}
			}
			asnTimerLogger.msgQueue.Remove(backElement)
		}
	}
	asnTimerLogger.processLogMsg(InfoLvl, itemArgs, cx)
}

/*func (asnTimerLogger *asyncTimerLoggerAgg) processQueueElement1() {

	var itemArgs items
	var cx LogContextInterface
	itemArgs.il = make(map[string]*logItem)

	if asnTimerLogger.msgQueue.Len() > 0 {
		backElement := asnTimerLogger.msgQueue.Front()
		msg, _ := backElement.Value.(msgQueueItem)
		cx = msg.context

		var funcLog logItem
		funcLog.functionName = msg.message.String()
		if v, ok := itemArgs.il[funcLog.functionName]; ok {
			v.count += 1
		} else {
			funcLog.count = 1
			itemArgs.il[funcLog.functionName] = &funcLog
		}
		asnTimerLogger.msgQueue.Remove(backElement)
	}
	asnTimerLogger.processLogMsg(InfoLvl, itemArgs, cx)
}*/

func (asnTimerLogger *asyncTimerLoggerAgg) processQueues() {
	for !asnTimerLogger.Closed() {
		closed := asnTimerLogger.processItems()

		if closed {
			break
		}

		<-time.After(asnTimerLogger.interval)
	}
}

func (asnLogger *asyncTimerLoggerAgg) innerLog(
	level LogLevel,
	context LogContextInterface,
	message fmt.Stringer) {
	//asnLogger.addMsgToQueue(level, context, message)
}

func (asnLogger *asyncTimerLoggerAgg) Close() {
	asnLogger.m.Lock()
	defer asnLogger.m.Unlock()

	if !asnLogger.Closed() {
		asnLogger.flushQueue(true)
		asnLogger.config.RootDispatcher.Flush()

		if err := asnLogger.config.RootDispatcher.Close(); err != nil {
			reportInternalError(err)
		}

		asnLogger.closedM.Lock()
		asnLogger.closed = true
		asnLogger.closedM.Unlock()
		asnLogger.queueHasElements.Broadcast()
	}
}

func (asnLogger *asyncTimerLoggerAgg) Flush() {
	asnLogger.m.Lock()
	defer asnLogger.m.Unlock()

	if !asnLogger.Closed() {
		asnLogger.flushQueue(true)
		asnLogger.config.RootDispatcher.Flush()
	}
}

func (asnLogger *asyncTimerLoggerAgg) flushQueue(lockNeeded bool) {
	if lockNeeded {
		asnLogger.queueHasElements.L.Lock()
		defer asnLogger.queueHasElements.L.Unlock()
	}

	for asnLogger.msgQueue.Len() > 0 {
		asnLogger.processQueueElements()
	}
}

func (asnLogger *asyncTimerLoggerAgg) addMsgToQueue(
	level LogLevel,
	context LogContextInterface,
	message interface{}) {

	if !asnLogger.Closed() {
		asnLogger.queueHasElements.L.Lock()
		defer asnLogger.queueHasElements.L.Unlock()

		if asnLogger.msgQueue.Len() >= MaxQueueSize {
			fmt.Printf("Seelog queue overflow: more than %v messages in the queue. Flushing.\n", MaxQueueSize)
			asnLogger.flushQueue(false)
		}

		queueItem := msgQueueMetric{level, context, message}

		asnLogger.msgQueue.PushBack(queueItem)
		asnLogger.queueHasElements.Broadcast()
	} else {
		err := fmt.Errorf("queue closed! Cannot process element: %d %#v", level, message)
		reportInternalError(err)
	}
}
