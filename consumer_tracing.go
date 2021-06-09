package kafka_zipkin_interceptor

import (
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/model"
	"github.com/segmentio/kafka-go"
	"strconv"
)

func ExtractTraceInfo(m kafka.Message, key string, topic string, clientId string, groupId string, t *zipkin.Tracer) zipkin.Span {
	traceId := ""
	spanId := ""

	for _, header := range m.Headers {
		if header.Key == "X-B3-TraceId" {
			traceId = string(header.Value)
		} else if header.Key == "X-B3-SpanId" {
			spanId = string(header.Value)
		}
	}

	unitSpanId, _ := strconv.ParseUint(spanId, 0, 64)
	traceIdModel, _ := model.TraceIDFromHex(traceId)

	spanContext := model.SpanContext{
		TraceID: traceIdModel,
		ID:      model.ID(unitSpanId),
	}

	tags := map[string]string {
		KAFKA_KEY: key,
		KAFKA_TOPIC: topic,
		KAFKA_CLIENT_ID: clientId,
		KAFKA_GROUP_ID: groupId,
	}

	span := t.StartSpan(
		SPAN_NAME_POLL,
		zipkin.RemoteEndpoint(&model.Endpoint{ServiceName: REMOTE_SERVICE_NAME_DEFAULT}),
		zipkin.Tags(tags),
		zipkin.Kind(model.Consumer),
		zipkin.Parent(spanContext),
	)

	return span
}
