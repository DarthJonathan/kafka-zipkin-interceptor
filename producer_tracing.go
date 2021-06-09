package kafka_zipkin_interceptor

import (
	"context"
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/segmentio/kafka-go"
)

var (
	REMOTE_SERVICE_NAME_DEFAULT = "kafka"
	KAFKA_TOPIC = "kafka.topic"
	KAFKA_KEY = "kafka.key"
	KAFKA_CLIENT_ID = "kafka.client.id"
	KAFKA_GROUP_ID = "kafka.group.id"
	SPAN_NAME_SEND = "send"
	SPAN_NAME_POLL = "poll"
)

func WrapInSpan(key string, topic string, clientId string, groupId string, ctx context.Context, t *zipkin.Tracer) ([]kafka.Header, context.Context) {
	tags := map[string]string {
		KAFKA_KEY: key,
		KAFKA_TOPIC: topic,
		KAFKA_CLIENT_ID: clientId,
		KAFKA_GROUP_ID: groupId,
	}

	span, ctx := t.StartSpanFromContext(
			ctx,
			SPAN_NAME_SEND,
			zipkin.RemoteEndpoint(&model.Endpoint{ServiceName: REMOTE_SERVICE_NAME_DEFAULT}),
			zipkin.Tags(tags),
			zipkin.Kind(model.Producer),
		)


	spanID := zipkin.SpanFromContext(ctx).Context().ID.String()
	traceID := zipkin.SpanFromContext(ctx).Context().TraceID.String()
	headers := []kafka.Header{
		{
			Key:   "X-B3-TraceId",
			Value: []byte(traceID),
		},
		{
			Key:   "X-B3-SpanId",
			Value: []byte(spanID),
		},
		{
			Key:   "X-B3-Sampled",
			Value: []byte("1"),
		},
	}

	defer span.Finish()
	return headers, ctx
}
