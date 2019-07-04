package tracing

import (
	"encoding/json"
	"github.com/itzujun/gocelery/tasks"
	"github.com/opentracing/opentracing-go"
	opentracing_ext "github.com/opentracing/opentracing-go/ext"
	opentracing_log "github.com/opentracing/opentracing-go/log"
)

var (
	MachineryTag     = opentracing.Tag{Key: string(opentracing_ext.Component), Value: "machinery"}
	WorkflowGroupTag = opentracing.Tag{Key: "machinery.workflow", Value: "group"}
	WorkflowChordTag = opentracing.Tag{Key: "machinery.workflow", Value: "chord"}
	WorkflowChainTag = opentracing.Tag{Key: "machinery.workflow", Value: "chain"}
)

func StartSpanFromHeaders(headers tasks.Headers, operationName string) opentracing.Span {
	spanContext, err := opentracing.GlobalTracer().Extract(opentracing.TextMap, headers)
	span := opentracing.StartSpan(
		operationName,
		ConsumerOption(spanContext),
		MachineryTag,
	)
	if err != nil {
		span.LogFields(opentracing_log.Error(err))
	}
	return span
}

func HeadersWithSpan(headers tasks.Headers, span opentracing.Span) tasks.Headers {
	if headers == nil {
		headers = make(tasks.Headers)
	}
	if err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.TextMap, headers); err != nil {
		span.LogFields(opentracing_log.Error(err))
	}
	return headers
}

type consumerOption struct {
	producerContext opentracing.SpanContext
}

func (c consumerOption) Apply(o *opentracing.StartSpanOptions) {
	if c.producerContext != nil {
		opentracing.FollowsFrom(c.producerContext).Apply(o)
	}
	opentracing_ext.SpanKindConsumer.Apply(o)
}

func ConsumerOption(producer opentracing.SpanContext) opentracing.StartSpanOption {
	return consumerOption{producer}
}

type producerOption struct{}

func (p producerOption) Apply(o *opentracing.StartSpanOptions) {
	opentracing_ext.SpanKindProducer.Apply(o)
}

func ProducerOption() opentracing.StartSpanOption {
	return producerOption{}
}

func AnnotateSpanWithSignatureInfo(span opentracing.Span, signature *tasks.Signature) {
	span.SetTag("signature.name", signature.Name)
	span.SetTag("signature.uuid", signature.UUID)
	if signature.GroupUUID != "" {
		span.SetTag("signature.group.uuid", signature.UUID)
	}

	if signature.ChordCallback != nil {
		span.SetTag("signature.chord.callback.uuid", signature.ChordCallback.UUID)
		span.SetTag("signature.chord.callback.name", signature.ChordCallback.Name)
	}
}

func AnnotateSpanWithChainInfo(span opentracing.Span, chain *tasks.Chain) {
	span.SetTag("chain.tasks.length", len(chain.Tasks))
	for _, signature := range chain.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

func AnnotateSpanWithGroupInfo(span opentracing.Span, group *tasks.Group, sendConcurrency int) {
	span.SetTag("group.uuid", group.GroupUUID)
	span.SetTag("group.tasks.length", len(group.Tasks))
	span.SetTag("group.concurrency", sendConcurrency)
	if taskUUIDs, err := json.Marshal(group.GetUUIDs()); err == nil {
		span.SetTag("group.tasks", string(taskUUIDs))
	} else {
		span.SetTag("group.tasks", group.GetUUIDs())
	}
	for _, signature := range group.Tasks {
		signature.Headers = HeadersWithSpan(signature.Headers, span)
	}
}

func AnnotateSpanWithChordInfo(span opentracing.Span, chord *tasks.Chord, sendConcurrency int) {
	span.SetTag("chord.callback.uuid", chord.Callback.UUID)
	chord.Callback.Headers = HeadersWithSpan(chord.Callback.Headers, span)
	AnnotateSpanWithGroupInfo(span, chord.Group, sendConcurrency)
}
