package gotred

import (
	"context"
	"time"
)

type ITracer interface {
	TraceDependency(
		ctx context.Context,
		spanId string,
		dependencyType string,
		serviceName string,
		commandName string,
		success bool,
		startTimestamp time.Time,
		eventTimestamp time.Time,
		fields map[string]string,
	)
}
