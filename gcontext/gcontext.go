package gcontext
import (
	"context"
	"google.golang.org/grpc/metadata"
)
const (
	TRACE_ID = "traceid"
)
type GContext struct {
	context.Context
	md metadata.MD
}
func FromContext(ctx context.Context) *GContext {
	if ctx == nil {
		ctx = context.Background()
	}
	return &GContext{
		Context: ctx,
	}
}
func (g *GContext) WithGrpcMeta(key, value string) *GContext {
	if g.md == nil {
		g.md = metadata.Pairs(key, value)
	} else {
		g.md[key] = append(g.md[key], value)
	}
	return g
}
func (g *GContext) WithTraceId(traceId string) *GContext {
	g.WithGrpcMeta(TRACE_ID, traceId)
	return g
}
func (g *GContext) Build() context.Context {
	return metadata.NewOutgoingContext(g, g.md)
}
func GetTraceIdFromMetadata(ctx context.Context) string {
	traceId, ok := GetTraceIdFromOutgoingContext(ctx)
	if ok == true {
		return traceId
	}
	traceId, _ = GetValueFromInComingMetadata(ctx)
	return traceId
}
func GetTraceIdFromOutgoingContext(ctx context.Context) (string, bool) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if ! ok {
		return  "", false
	}
	if tIds, ok := md[TRACE_ID]; ok {
		if len(tIds) > 0 {
			return tIds[0], true
		}
	}
	return "", false
}
func GetValueFromInComingMetadata(ctx context.Context) (string, bool) {
	md, exist := metadata.FromIncomingContext(ctx)
	if !exist {
		return "", false
	}
	if tIds, ok := md[TRACE_ID]; ok {
		if len(tIds) > 0 {
			return tIds[0], true
		}
	}
	return "", false
}
