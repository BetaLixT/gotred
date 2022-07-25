package gotred

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

const (
	START_TIME_KEY = "gtrd:start"
)

type TraceHook struct {
	tracer ITracer
	serviceName string
}

var _ redis.Hook = (*TraceHook)(nil)

func (hook *TraceHook) BeforeProcess(
	ctx context.Context,
	cmd redis.Cmder,
) (context.Context, error) {
	return context.WithValue(ctx, START_TIME_KEY, time.Now()), nil
}

func (hook *TraceHook) AfterProcess(
	ctx context.Context,
	cmd redis.Cmder,
) error {
	end := time.Now()
	start := end
	raw := ctx.Value(START_TIME_KEY)
	if raw != nil {
		if cstd, ok := raw.(time.Time); ok {
			start = cstd
		}
	}

	fields := map[string]string{}
	err := cmd.Err()	
	if err != nil {
		fields["error"] = err.Error()
	}
	hook.tracer.TraceDependency(
		ctx,
		"",
		"Redis",
		hook.serviceName,
		cmd.String(),
		err == nil,
		start,
		end,
		fields,
	)
	return nil
}

func (hook *TraceHook) BeforeProcessPipeline(
	ctx context.Context,
	cmds []redis.Cmder,
) (context.Context, error) {
	return context.WithValue(ctx, START_TIME_KEY, time.Now()), nil
}
func (hook *TraceHook) AfterProcessPipeline(
	ctx context.Context,
	cmds []redis.Cmder,
) error {
	end := time.Now()
	start := end
	raw := ctx.Value(START_TIME_KEY)
	if raw != nil {
		if cstd, ok := raw.(time.Time); ok {
			start = cstd
		}
	}
	fields := map[string]string{}
	cmdstr := ""
	for idx, cmd := range cmds {
		err := cmd.Err()
		if err != nil {
			fields[fmt.Sprintf("error:%d", idx)] = err.Error()
			
		}
		cmdstr = cmdstr + cmd.String() + ";"
	}
	hook.tracer.TraceDependency(
		ctx,
		"",
		"Redis",
		hook.serviceName,
		cmdstr,
		len(fields) == 0,
		start,
		end,
		fields,
	)
	return nil
}

