// Copyright 2019, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package translator

import (
	"strings"
	"testing"
	"time"

	tracepb "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/consumer/pdata"
	semconventions "go.opentelemetry.io/collector/translator/conventions"
)

func TestCauseWithExceptions(t *testing.T) {
	errorMsg := "this is a test"
	attributeMap := make(map[string]interface{})
	attributeMap[semconventions.AttributeHTTPMethod] = "POST"
	attributeMap[semconventions.AttributeHTTPURL] = "https://api.example.com/widgets"
	attributeMap[semconventions.AttributeHTTPStatusCode] = 500

	event1 := pdata.NewSpanEvent()
	event1.InitEmpty()
	event1.SetName(semconventions.AttributeExceptionEventName)
	attributes := pdata.NewAttributeMap()
	attributes.InsertString(semconventions.AttributeExceptionType, "java.lang.IllegalStateException")
	attributes.InsertString(semconventions.AttributeExceptionMessage, "bad state")
	attributes.CopyTo(event1.Attributes())

	event2 := pdata.NewSpanEvent()
	event2.InitEmpty()
	event2.SetName(semconventions.AttributeExceptionEventName)
	attributes = pdata.NewAttributeMap()
	attributes.InsertString(semconventions.AttributeExceptionType, "EmptyError")
	attributes.CopyTo(event2.Attributes())

	span := constructExceptionServerSpan(attributeMap, pdata.StatusCode(tracepb.Status_InternalError))
	span.Status().SetMessage(errorMsg)
	span.Events().Append(&event1)
	span.Events().Append(&event2)
	filtered, _ := makeHTTP(span)

	isError, isFault, filteredResult, cause := makeCause(span, filtered)

	assert.False(t, isError)
	assert.True(t, isFault)
	assert.Equal(t, filtered, filteredResult)
	assert.NotNil(t, cause)
	assert.Len(t, cause.Exceptions, 2)
	assert.NotEmpty(t, cause.Exceptions[0].ID)
	assert.Equal(t, "java.lang.IllegalStateException", cause.Exceptions[0].Type)
	assert.Equal(t, "bad state", cause.Exceptions[0].Message)
	assert.NotEmpty(t, cause.Exceptions[1].ID)
	assert.Equal(t, "EmptyError", cause.Exceptions[1].Type)
	assert.Empty(t, cause.Exceptions[1].Message)
}

func TestCauseWithStatusMessage(t *testing.T) {
	errorMsg := "this is a test"
	attributes := make(map[string]interface{})
	attributes[semconventions.AttributeHTTPMethod] = "POST"
	attributes[semconventions.AttributeHTTPURL] = "https://api.example.com/widgets"
	attributes[semconventions.AttributeHTTPStatusCode] = 500
	span := constructExceptionServerSpan(attributes, pdata.StatusCode(tracepb.Status_InternalError))
	span.Status().SetMessage(errorMsg)
	filtered, _ := makeHTTP(span)

	isError, isFault, filtered, cause := makeCause(span, filtered)

	assert.False(t, isError)
	assert.True(t, isFault)
	assert.NotNil(t, filtered)
	assert.NotNil(t, cause)
	w := testWriters.borrow()
	if err := w.Encode(cause); err != nil {
		assert.Fail(t, "invalid json")
	}
	jsonStr := w.String()
	testWriters.release(w)
	assert.True(t, strings.Contains(jsonStr, errorMsg))
}

func TestCauseWithHttpStatusMessage(t *testing.T) {
	errorMsg := "this is a test"
	attributes := make(map[string]interface{})
	attributes[semconventions.AttributeHTTPMethod] = "POST"
	attributes[semconventions.AttributeHTTPURL] = "https://api.example.com/widgets"
	attributes[semconventions.AttributeHTTPStatusCode] = 500
	attributes[semconventions.AttributeHTTPStatusText] = errorMsg
	span := constructExceptionServerSpan(attributes, pdata.StatusCode(tracepb.Status_InternalError))
	filtered, _ := makeHTTP(span)

	isError, isFault, filtered, cause := makeCause(span, filtered)

	assert.False(t, isError)
	assert.True(t, isFault)
	assert.NotNil(t, filtered)
	assert.NotNil(t, cause)
	w := testWriters.borrow()
	if err := w.Encode(cause); err != nil {
		assert.Fail(t, "invalid json")
	}
	jsonStr := w.String()
	testWriters.release(w)
	assert.True(t, strings.Contains(jsonStr, errorMsg))
}

func TestCauseWithZeroStatusMessage(t *testing.T) {
	errorMsg := "this is a test"
	attributes := make(map[string]interface{})
	attributes[semconventions.AttributeHTTPMethod] = "POST"
	attributes[semconventions.AttributeHTTPURL] = "https://api.example.com/widgets"
	attributes[semconventions.AttributeHTTPStatusCode] = 500
	attributes[semconventions.AttributeHTTPStatusText] = errorMsg

	span := constructExceptionServerSpan(attributes, pdata.StatusCode(tracepb.Status_Ok))
	filtered, _ := makeHTTP(span)
	// Status is used to determine whether an error or not.
	// This span illustrates incorrect instrumentation,
	// marking a success status with an error http status code, and status wins.
	// We do not expect to see such spans in practice.
	isError, isFault, filtered, cause := makeCause(span, filtered)

	assert.False(t, isError)
	assert.False(t, isFault)
	assert.NotNil(t, filtered)
	assert.Nil(t, cause)
}

func TestCauseWithClientErrorMessage(t *testing.T) {
	errorMsg := "this is a test"
	attributes := make(map[string]interface{})
	attributes[semconventions.AttributeHTTPMethod] = "POST"
	attributes[semconventions.AttributeHTTPURL] = "https://api.example.com/widgets"
	attributes[semconventions.AttributeHTTPStatusCode] = 499
	attributes[semconventions.AttributeHTTPStatusText] = errorMsg

	span := constructExceptionServerSpan(attributes, pdata.StatusCode(tracepb.Status_Cancelled))
	filtered, _ := makeHTTP(span)

	isError, isFault, filtered, cause := makeCause(span, filtered)

	assert.True(t, isError)
	assert.False(t, isFault)
	assert.NotNil(t, filtered)
	assert.NotNil(t, cause)
}

func constructExceptionServerSpan(attributes map[string]interface{}, statuscode pdata.StatusCode) pdata.Span {
	endTime := time.Now().Round(time.Second)
	startTime := endTime.Add(-90 * time.Second)
	spanAttributes := constructSpanAttributes(attributes)

	span := pdata.NewSpan()
	span.InitEmpty()
	span.SetTraceID(newTraceID())
	span.SetSpanID(newSegmentID())
	span.SetParentSpanID(newSegmentID())
	span.SetName("/widgets")
	span.SetKind(pdata.SpanKindSERVER)
	span.SetStartTime(pdata.TimestampUnixNano(startTime.UnixNano()))
	span.SetEndTime(pdata.TimestampUnixNano(endTime.UnixNano()))

	status := pdata.NewSpanStatus()
	status.InitEmpty()
	status.SetCode(pdata.StatusCode(statuscode))
	status.CopyTo(span.Status())

	spanAttributes.CopyTo(span.Attributes())
	return span
}
