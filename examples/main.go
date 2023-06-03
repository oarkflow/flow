package main

import (
	"context"
	"encoding/json"

	"github.com/oarkflow/asynq"
	"github.com/oarkflow/xid"

	"github.coom/oarkflow/flow"
)

const redisAddrWorker = "127.0.0.1:6379"

func main() {
	// send(asynq.Sync)
	send(asynq.Async)
}
func send(mode asynq.Mode) {
	f := flow.NewFlow(xid.New().String())
	f.FirstNode = "get:input"
	f.AddHandler("email:deliver", &EmailDelivery{Operation{Type: "process"}})
	f.AddHandler("prepare:email", &PrepareEmail{Operation{Type: "process"}})
	f.AddHandler("get:input", &GetData{Operation{Type: "input"}})
	f.AddHandler("loop", &Loop{Operation{Type: "loop"}})
	f.AddHandler("condition", &Condition{Operation{Type: "condition"}})
	f.AddHandler("store:data", &StoreData{Operation{Type: "process"}})
	f.AddHandler("send:sms", &SendSms{Operation{Type: "process"}})
	f.AddHandler("notification", &InAppNotification{Operation{Type: "process"}})
	f.AddBranch("condition", map[string]string{
		"pass": "email:deliver",
		"fail": "store:data",
	})
	f.AddEdge("get:input", "loop")
	f.AddLoop("loop", "prepare:email")
	f.AddEdge("prepare:email", "condition")
	f.AddEdge("store:data", "send:sms")
	f.AddEdge("store:data", "notification")
	data := []map[string]any{
		{
			"phone": "+123456789",
			"email": "abc.xyz@gmail.com",
		},
		{
			"phone": "+98765412",
			"email": "xyz.abc@gmail.com",
		},
	}
	bt, _ := json.Marshal(data)
	task := asynq.NewTask("get:input", bt)
	f.ProcessTask(context.Background(), task)
}
