package flow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/oarkflow/asynq"
	"github.com/oarkflow/xid"
	"golang.org/x/sync/errgroup"
)

type node struct {
	id      string
	handler asynq.Handler
	params  map[string]any
	loops   []string
	edges   []string
	flow    *Flow
}

func NewNode(id string, handler asynq.Handler, params ...map[string]any) *node {
	n := &node{
		id:      id,
		handler: handler,
	}
	if len(params) > 0 {
		n.params = params[0]
	}
	return n
}

func (n *node) loop(ctx context.Context, payload []byte) ([]any, error) {
	extraParams := map[string]any{}
	g, ctx := errgroup.WithContext(ctx)
	ep := ctx.Value("extra_params")
	switch ep := ep.(type) {
	case map[string]any:
		extraParams = ep
	case string:
		json.Unmarshal([]byte(ep), &extraParams)
	}
	result := make(chan interface{})
	var rs, results []interface{}
	err := json.Unmarshal(payload, &rs)
	if err != nil {
		panic(string(payload))
		return nil, err
	}
	for _, single := range rs {
		single := single
		g.Go(func() error {
			var payload []byte
			currentData := make(map[string]any)
			switch s := single.(type) {
			case map[string]any:
				if len(extraParams) > 0 {
					for k, v := range extraParams {
						s[k] = v
					}
				}
				id := xid.New().String()
				if _, ok := s[n.flow.Config.idKey]; !ok {
					s[n.flow.Config.idKey] = id
				}
				if _, ok := s[n.flow.Config.flowIDKey]; !ok {
					s[n.flow.Config.flowIDKey] = n.flow.ID
				}
				if _, ok := s[n.flow.Config.statusKey]; !ok {
					s[n.flow.Config.statusKey] = "pending"
				}
				currentData = s
				payload, err = json.Marshal(currentData)
				if err != nil {
					return err
				}
				break
			default:
				payload, err = json.Marshal(single)
				if err != nil {
					return err
				}
			}
			var responseData map[string]interface{}
			for _, v := range n.loops {
				t := asynq.NewTask(v, payload, asynq.FlowID(n.flow.ID), asynq.Queue(v))
				res := n.flow.processNode(ctx, t, n.flow.nodes[v])
				err = json.Unmarshal(res.Data, &responseData)
				if err != nil {
					return err
				}
				currentData = mergeMap(currentData, responseData)
			}
			payload, err = json.Marshal(currentData)
			if err != nil {
				return err
			}
			err = json.Unmarshal(payload, &single)
			if err != nil {
				return err
			}
			select {
			case result <- single:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})
	}
	go func() {
		g.Wait()
		close(result)
	}()
	for ch := range result {
		results = append(results, ch)
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

func (n *node) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	result := n.handler.ProcessTask(ctx, task)
	if result.Error != nil {
		return result
	}
	if len(n.loops) > 0 {
		arr, err := n.loop(ctx, result.Data)
		if err != nil {
			result.Error = err
			return result
		}
		bt, err := json.Marshal(arr)
		result.Data = bt
		result.Error = err
	}

	return result
}

func (n *node) GetType() string {
	return n.handler.GetType()
}

func (n *node) GetKey() string {
	return n.handler.GetKey()
}

type Engine struct{}

type Config struct {
	idKey        string
	statusKey    string
	operationKey string
	flowIDKey    string
}

type Flow struct {
	Key       string
	ID        string
	FirstNode string
	Nodes     []string
	Edges     [][]string
	Loops     [][]string
	Branches  map[string]map[string]string
	Config    Config
	nodes     map[string]*node
	firstNode *node
	path      [][]string
	mu        sync.RWMutex
}

func NewFlow(id string) *Flow {
	return &Flow{ID: id, nodes: make(map[string]*node), Branches: make(map[string]map[string]string), Config: Config{
		idKey:     "asynq_id",
		statusKey: "asynq_status",
		flowIDKey: "flow_id",
	}}
}

func (f *Flow) AddHandler(id string, handler asynq.Handler, params ...map[string]any) *Flow {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := &node{
		id:      id,
		handler: handler,
		flow:    f,
	}
	if len(params) > 0 {
		n.params = params[0]
	}
	f.nodes[id] = n
	f.Nodes = append(f.Nodes, id)
	return f
}

func (f *Flow) AddEdge(in, out string) {
	edge := []string{in, out}
	f.Edges = append(f.Edges, edge)
}

func (f *Flow) AddLoop(in string, out ...string) {
	loop := []string{in}
	loop = append(loop, out...)
	f.Loops = append(f.Loops, loop)
}

func (f *Flow) AddBranch(id string, branch map[string]string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Branches[id] = branch
}

func (f *Flow) processNode(ctx context.Context, task *asynq.Task, n *node) asynq.Result {
	result := n.ProcessTask(ctx, task)
	if result.Error != nil {
		return result
	}
	if n.GetType() == "condition" {
		if ft, ok := f.Branches[n.id]; ok && result.Status != "" {
			if c, o := ft[result.Status]; o {
				t := asynq.NewTask(c, result.Data, asynq.FlowID(f.ID))
				result = f.processNode(ctx, t, f.nodes[c])
			}
		}
	}
	edgeResult := make(map[string][]byte)
	for _, edge := range n.edges {
		if nd, ok := f.nodes[edge]; ok {
			newTask := asynq.NewTask(edge, result.Data, asynq.FlowID(n.flow.ID), asynq.Queue(edge))
			r := f.processNode(ctx, newTask, nd)
			if r.Error != nil {
				return r
			}
			edgeResult[edge+"_result"] = r.Data
		}
	}
	totalResults := len(edgeResult)
	for _, r := range edgeResult {
		if totalResults == 1 {
			result.Data = r
			return result
		}
	}
	if totalResults == 0 {
		return result
	}
	edgeResult[n.id+"_result"] = result.Data
	data := make(map[string]any)
	for key, val := range edgeResult {
		d, _, err := AsMap(val)
		if err != nil {
			result.Error = err
			return result
		}
		data[key] = d
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		result.Error = err
		return result
	}
	result.Data = bytes
	return result
}

func (f *Flow) ProcessTask(ctx context.Context, task *asynq.Task) asynq.Result {
	f.prepareNodes()
	if f.firstNode == nil {
		return asynq.Result{Error: errors.New("Provide initial handler")}
	}
	return f.processNode(ctx, task, f.firstNode)
}

func (f *Flow) GetType() string {
	return "flow"
}

func (f *Flow) GetKey() string {
	return f.Key
}

func (f *Flow) prepareNodes() {
	var src, dest []string
	for _, edge := range f.Edges {
		in := edge[0]
		out := edge[1]
		src = append(src, in)
		dest = append(dest, out)
		if node, ok := f.nodes[in]; ok {
			node.edges = append(node.edges, out)
		}
	}
	for _, loop := range f.Loops {
		in := loop[0]
		out := loop[1:]
		src = append(src, in)
		dest = append(dest, out...)
		if node, ok := f.nodes[in]; ok {
			node.loops = append(node.loops, out...)
		}
	}
	if f.FirstNode == "" {
		for _, t := range src {
			if !contains(dest, t) {
				f.FirstNode = t
			}
		}
	}
	if f.FirstNode != "" {
		f.firstNode = f.nodes[f.FirstNode]
	}
}

func contains[T comparable](s []T, v T) bool {
	for _, vv := range s {
		if vv == v {
			return true
		}
	}
	return false
}

func mergeMap(map1 map[string]any, map2 map[string]any) map[string]any {
	for k, m := range map2 {
		if _, ok := map1[k]; !ok {
			map1[k] = m
		}
	}
	return map1
}

func AsMap(payload []byte) (data any, slice bool, err error) {
	var mp map[string]any
	err = json.Unmarshal(payload, &mp)
	if err != nil {
		var mps []map[string]any
		err = json.Unmarshal(payload, &mps)
		if err == nil {
			data = mps
			slice = true
		}
	} else {
		data = mp
	}
	return
}
