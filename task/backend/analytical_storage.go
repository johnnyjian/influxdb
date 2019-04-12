package backend

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	// temp for displaying
	"github.com/influxdata/flux"
	"github.com/influxdata/flux/execute"
	"os"

	"github.com/influxdata/flux/lang"
	"github.com/influxdata/influxdb"
	pctx "github.com/influxdata/influxdb/context"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/storage"
	"github.com/influxdata/influxdb/tsdb"
)

// NewAnalyticalStorage creates a new analytical store with access to the necessary systems for storing data and to act as a middleware
func NewAnalyticalStorage(ts influxdb.TaskService, tcs TaskControlService, pw storage.PointsWriter, qs query.QueryService) *AnalyticalStorage {
	return &AnalyticalStorage{
		TaskService:        ts,
		TaskControlService: tcs,
		pw:                 pw,
		qs:                 qs,
	}
}

type AnalyticalStorage struct {
	influxdb.TaskService
	TaskControlService

	pw storage.PointsWriter
	qs query.QueryService
}

// type Run struct {
// 	ScheduledFor string `json:"scheduledFor"` // record time

// 	TaskID       ID     `json:"taskID"`
// 	Status       string `json:"status"`

// 	ID           ID     `json:"id,omitempty"`
// 	StartedAt    string `json:"startedAt,omitempty"`
// 	FinishedAt   string `json:"finishedAt,omitempty"`
// 	RequestedAt  string `json:"requestedAt,omitempty"`
// 	Log          []Log  `json:"log"`
// }

func (as *AnalyticalStorage) FinishRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	run, err := as.TaskControlService.FinishRun(ctx, taskID, runID)
	if run != nil {
		task, err := as.TaskService.FindTaskByID(ctx, run.TaskID)
		if err != nil {
			return run, err
		}

		tags := models.Tags{
			models.NewTag([]byte(taskIDTag), []byte(run.TaskID.String())),
			models.NewTag([]byte(statusField), []byte(run.Status)),
		}

		fields := map[string]interface{}{}
		fields[statusField] = run.Status
		fields[runIDField] = run.ID.String()
		fields[startedAtField] = run.StartedAt
		fields[finishedAtField] = run.FinishedAt
		if run.RequestedAt != "" {
			fields[requestedAtField] = run.RequestedAt
		}

		sf, err := run.ScheduledForTime()
		if err != nil {
			return run, err
		}

		logBytes, err := json.Marshal(run.Log)
		if err != nil {
			return run, err
		}
		fields[logField] = string(logBytes)

		point, err := models.NewPoint("runs", tags, fields, sf)
		if err != nil {
			panic(err)
		}

		// bucketID := influxdb.ID(0)
		// if bs, ok := as.TaskService.(influxdb.BucketService); ok {
		// 	name := ("system")
		// 	bucket, _ := bs.FindBucket(ctx, influxdb.BucketFilter{
		// 		Name:           &name,
		// 		OrganizationID: &task.OrganizationID,
		// 	})

		// 	if bucket == nil {
		// 		bucket = &influxdb.Bucket{OrganizationID: task.OrganizationID, Name: "system"}
		// 		err := bs.CreateBucket(ctx, bucket)
		// 		if err != nil {
		// 			panic(err)
		// 		}
		// 	}
		// 	bucketID = bucket.ID
		// }

		// use the tsdb explode points to convert to the new style.
		// We could split this on our own but its quite possible this could change.
		points, err := tsdb.ExplodePoints(task.OrganizationID, taskSystemBucketID, models.Points{point})
		if err != nil {
			panic(err)
		}
		err = as.pw.WritePoints(ctx, points)
		fmt.Println("err", err)
		fmt.Printf("run finished %+v\n", run)
		for _, point := range points {
			fmt.Println("point: ", point.String())
		}
		return run, err
	}
	return run, err
}

// FindLogs returns logs for a run.
// First attempt to use the TaskService, then append additional analytical's logs to the list
func (as *AnalyticalStorage) FindLogs(ctx context.Context, filter influxdb.LogFilter) ([]*influxdb.Log, int, error) {
	logs, n, err := as.TaskService.FindLogs(ctx, filter)
	if err != nil {
		if err, ok := err.(*influxdb.Error); !ok || err.Msg != "run not found" {
			return logs, n, err
		}
	}

	task, err := as.TaskService.FindTaskByID(ctx, filter.Task)
	if err != nil {
		return logs, n, err
	}

	filterPart := ""
	if filter.Run != nil {
		filterPart = fmt.Sprintf(`|> filter(fn: (r) => r.runID == %q)`, filter.Run.String())
	}

	// TODO(lh): Change the range to something more reasonable. Not sure what that range will be.
	listScript := fmt.Sprintf(`from(bucket: "system")
	  |> range(start: -100000d)
	  |> filter(fn: (r) => r._measurement == "runs" and r.taskID == %q)
	  %s
	  `, filter.Task.String(), filterPart)
	// listScript = "buckets()"
	fmt.Println("listScript", listScript)

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		fmt.Println("error", err)
		return nil, 0, err
	}
	if auth.Kind() != "authorization" {
		fmt.Println("errorers")
		return nil, 0, influxdb.ErrAuthorizerNotSupported
	}
	request := &query.Request{Authorization: auth.(*influxdb.Authorization), OrganizationID: task.OrganizationID, Compiler: lang.FluxCompiler{Query: listScript}}

	ittr, err := as.qs.Query(ctx, request)
	if err != nil {
		fmt.Println("errar", err)
		return nil, 0, err
	}
	defer ittr.Release()

	{
		for ittr.More() {
			result := ittr.Next()
			tables := result.Tables()
			fmt.Println("Result:", result.Name())
			err := tables.Do(func(tbl flux.Table) error {
				_, err := execute.NewFormatter(tbl, nil).WriteTo(os.Stdout)
				panic(err)
			})
			if err != nil {
				panic(err)
			}
		}
	}

	return logs, n, err
}

// FindRuns returns a list of runs that match a filter and the total count of returned runs.
// First attempt to use the TaskService, then append additional analytical's runs to the list
func (as *AnalyticalStorage) FindRuns(ctx context.Context, filter influxdb.RunFilter) ([]*influxdb.Run, int, error) {
	if filter.Limit == 0 || filter.Limit > influxdb.TaskMaxPageSize {
		filter.Limit = influxdb.TaskMaxPageSize
	}

	runs, n, err := as.TaskService.FindRuns(ctx, filter)
	if err != nil {
		return runs, n, err
	}

	// if we reached the limit lets stop here
	if len(runs) >= filter.Limit {
		return runs, n, err
	}

	task, err := as.TaskService.FindTaskByID(ctx, filter.Task)
	if err != nil {
		return runs, n, err
	}

	filterPart := ""
	if filter.After != nil {
		filterPart = fmt.Sprintf(`|> filter(fn: (r) => r.runID > %q)`, filter.After.String())
	}

	// TODO(lh): Change the range to something more reasonable. Not sure what that range will be.
	runsScript := fmt.Sprintf(`from(bucketID: "000000000000000a")
	  |> range(start: -100000d)
	  |> filter(fn: (r) => r._measurement == "runs" and r.taskID == %q)
	  %s
	  `, filter.Task.String(), filterPart)
	// runsScript = "buckets()"
	fmt.Println("runsScript", runsScript)
	time.Sleep(10 * time.Second)
	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		fmt.Println("error", err)
		return nil, 0, err
	}
	if auth.Kind() != "authorization" {
		fmt.Println("errorers")
		return nil, 0, influxdb.ErrAuthorizerNotSupported
	}
	request := &query.Request{Authorization: auth.(*influxdb.Authorization), OrganizationID: task.OrganizationID, Compiler: lang.FluxCompiler{Query: runsScript}}

	ittr, err := as.qs.Query(ctx, request)
	if err != nil {
		fmt.Println("errar", err)
		return nil, 0, err
	}
	defer ittr.Release()

	{
		fmt.Println("runs script response", task.ID)
		for ittr.More() {
			result := ittr.Next()
			tables := result.Tables()
			fmt.Println("Result:", result.Name())
			err := tables.Do(func(tbl flux.Table) error {
				_, err := execute.NewFormatter(tbl, nil).WriteTo(os.Stdout)
				panic(err)
			})
			if err != nil {
				panic(err)
			}
		}
	}

	return runs, n, err
}

// FindRunByID returns a single run.
// First see if it is in the existing TaskService. If not pull it from analytical storage.
func (as *AnalyticalStorage) FindRunByID(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	// check the taskService to see if the run is on its list
	run, err := as.TaskService.FindRunByID(ctx, taskID, runID)
	if err != nil {
		if err, ok := err.(*influxdb.Error); !ok || err.Msg != "run not found" {
			return run, err
		}
	}

	task, err := as.TaskService.FindTaskByID(ctx, taskID)
	if err != nil {
		return run, err
	}

	// TODO(lh): Change the range to something more reasonable. Not sure what that range will be.
	findRunScript := fmt.Sprintf(`from(bucketID: "000000000000000a")
	  |> range(start: -100000d)
	  |> filter(fn: (r) => r._measurement == "runs" and r.taskID == %q and r.runID == %q)
	  `, taskID.String(), runID.String())
	// findRunScript = "buckets()"
	fmt.Println("findRunScript", findRunScript)

	auth, err := pctx.GetAuthorizer(ctx)
	if err != nil {
		fmt.Println("error", err)
		return nil, err
	}
	if auth.Kind() != "authorization" {
		fmt.Println("errorers")
		return nil, influxdb.ErrAuthorizerNotSupported
	}
	request := &query.Request{Authorization: auth.(*influxdb.Authorization), OrganizationID: task.OrganizationID, Compiler: lang.FluxCompiler{Query: findRunScript}}

	ittr, err := as.qs.Query(ctx, request)
	if err != nil {
		fmt.Println("errar", err)
		return nil, err
	}
	defer ittr.Release()

	{
		fmt.Println("find run results:")
		for ittr.More() {
			result := ittr.Next()
			tables := result.Tables()
			fmt.Println("Result:", result.Name())
			err := tables.Do(func(tbl flux.Table) error {
				_, err := execute.NewFormatter(tbl, nil).WriteTo(os.Stdout)
				panic(err)
			})
			if err != nil {
				panic(err)
			}
		}
	}

	return run, err
}

func (as *AnalyticalStorage) RetryRun(ctx context.Context, taskID, runID influxdb.ID) (*influxdb.Run, error) {
	return as.TaskService.RetryRun(ctx, taskID, runID)
}
