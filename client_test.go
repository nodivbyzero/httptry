package httptry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientRetriesStatusAndReplaysBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != "payload" {
			t.Errorf("body = %q", body)
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	client := NewClient(WithAttempts(2), WithDelayFunc(func(int, error) time.Duration { return 0 }))
	req, err := http.NewRequest(http.MethodPost, server.URL, strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("POST should not retry by default; calls = %d", got)
	}

	calls.Store(0)
	client = NewClient(WithAttempts(2), WithUnsafeMethods(), WithDelayFunc(func(int, error) time.Duration { return 0 }))
	req, _ = http.NewRequest(http.MethodPost, server.URL, strings.NewReader("payload"))
	_, err = client.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("calls = %d, want 2 after unsafe POST retry", got)
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	if !DefaultRetryPolicy(&http.Response{StatusCode: http.StatusServiceUnavailable}, nil) {
		t.Fatal("503 should retry")
	}
	if DefaultRetryPolicy(&http.Response{StatusCode: http.StatusNotImplemented}, nil) {
		t.Fatal("501 should not retry")
	}
	if DefaultRetryPolicy(nil, context.Canceled) {
		t.Fatal("cancellation should not retry")
	}
}

func TestWithRetryMaxCountsRetriesAfterInitialAttempt(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	client := NewClient(WithRetryMax(1), WithDelayFunc(func(int, error) time.Duration { return 0 }))
	resp, err := client.Get(server.URL)
	if err == nil || resp != nil {
		t.Fatalf("expected exhausted retry error, got resp=%v err=%v", resp, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
}

type testLogger struct{ messages []string }

func (l *testLogger) Printf(format string, args ...interface{}) {
	l.messages = append(l.messages, fmt.Sprintf(format, args...))
}

type injectedAttempt struct {
	response *http.Response
	err      error
}

type responseHeaderError struct{}

func (responseHeaderError) Error() string { return "response headers unavailable" }

type failureInjectionTransport struct {
	attempts []injectedAttempt
	calls    atomic.Int32
}

type deterministicClock struct {
	now    time.Time
	delays []time.Duration
}

type blockingClock struct {
	now     time.Time
	started chan struct{}
	tick    chan time.Time
}

func (c *blockingClock) Now() time.Time { return c.now }

func (c *blockingClock) After(time.Duration) <-chan time.Time {
	select {
	case <-c.started:
	default:
		close(c.started)
	}
	return c.tick
}

type trackedBody struct {
	reader *strings.Reader
	closed atomic.Bool
}

func (b *trackedBody) Read(p []byte) (int, error) { return b.reader.Read(p) }
func (b *trackedBody) Close() error {
	b.closed.Store(true)
	return nil
}

func (c *deterministicClock) Now() time.Time { return c.now }

func (c *deterministicClock) After(delay time.Duration) <-chan time.Time {
	c.delays = append(c.delays, delay)
	tick := make(chan time.Time, 1)
	tick <- c.now.Add(delay)
	return tick
}

type failingBody struct {
	data    []byte
	readErr error
}

func (b *failingBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, b.readErr
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	return n, b.readErr
}
func (b failingBody) Close() error { return nil }

func (t *failureInjectionTransport) RoundTrip(*http.Request) (*http.Response, error) {
	index := int(t.calls.Add(1)) - 1
	if index >= len(t.attempts) {
		return nil, errors.New("failure injection: unexpected attempt")
	}
	attempt := t.attempts[index]
	return attempt.response, attempt.err
}

func injectedResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d", status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func TestFailureInjectionTransportRetriesTransportError(t *testing.T) {
	transport := &failureInjectionTransport{attempts: []injectedAttempt{
		{err: errors.New("connection reset")},
		{response: injectedResponse(http.StatusOK, "ok")},
	}}
	client := NewClient(
		WithHTTPClient(&http.Client{Transport: transport}),
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
	)
	resp, err := client.Get("http://injected.test")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if transport.calls.Load() != 2 {
		t.Fatalf("transport calls = %d, want 2", transport.calls.Load())
	}
}

func TestFailureInjectionTransportRetriesResponseHeaderError(t *testing.T) {
	transport := &failureInjectionTransport{attempts: []injectedAttempt{
		{err: responseHeaderError{}},
		{response: injectedResponse(http.StatusOK, "ok")},
	}}
	var observed error
	client := NewClient(
		WithHTTPClient(&http.Client{Transport: transport}),
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
		WithOnRetry(func(info AttemptInfo) { observed = info.Err }),
	)
	resp, err := client.Get("http://injected.test")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	var headerErr responseHeaderError
	if !errors.As(observed, &headerErr) {
		t.Fatalf("observed retry error = %v, want response-header error", observed)
	}
	if transport.calls.Load() != 2 {
		t.Fatalf("transport calls = %d, want 2", transport.calls.Load())
	}
}

func TestResponseBodyReadFailureIsNotRetried(t *testing.T) {
	bodyErr := errors.New("response body read failed")
	transport := &failureInjectionTransport{attempts: []injectedAttempt{
		{response: &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       &failingBody{readErr: bodyErr},
		}},
		{response: injectedResponse(http.StatusOK, "unexpected retry")},
	}}
	client := NewClient(
		WithHTTPClient(&http.Client{Transport: transport}),
		WithRetryMax(1),
	)
	resp, err := client.Get("http://injected.test")
	if err != nil {
		t.Fatal(err)
	}
	_, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !errors.Is(readErr, bodyErr) {
		t.Fatalf("read error = %v, want %v", readErr, bodyErr)
	}
	if transport.calls.Load() != 1 {
		t.Fatalf("transport calls = %d, want 1", transport.calls.Load())
	}
}

func TestPartialResponseBodyReadFailureIsNotRetried(t *testing.T) {
	bodyErr := errors.New("partial response body read failed")
	transport := &failureInjectionTransport{attempts: []injectedAttempt{{
		response: &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       &failingBody{data: []byte("partial"), readErr: bodyErr},
		},
	}}}
	client := NewClient(WithHTTPClient(&http.Client{Transport: transport}))
	resp, err := client.Get("http://injected.test")
	if err != nil {
		t.Fatal(err)
	}
	data, readErr := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(data) != "partial" || !errors.Is(readErr, bodyErr) {
		t.Fatalf("body = %q, read error = %v", data, readErr)
	}
	if transport.calls.Load() != 1 {
		t.Fatalf("transport calls = %d, want 1", transport.calls.Load())
	}
}

func TestMalformedRetryAfterFallsBackToConfiguredBackoff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "not-a-delay")
		calls.Add(1)
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	var retryDelay time.Duration
	client := NewClient(
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
		WithOnRetry(func(info AttemptInfo) { retryDelay = info.Delay }),
	)
	resp, err := client.Get(server.URL)
	if resp != nil || err == nil {
		t.Fatalf("expected exhausted retry error, got resp=%v err=%v", resp, err)
	}
	if calls.Load() != 2 || retryDelay != 0 {
		t.Fatalf("calls=%d retry delay=%s", calls.Load(), retryDelay)
	}
}

func TestDeterministicClockControlsBackoff(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	clock := &deterministicClock{now: time.Now()}
	client := NewClient(
		WithRetryMax(1),
		WithClock(clock),
		WithDelayFunc(func(int, error) time.Duration { return 750 * time.Millisecond }),
	)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if len(clock.delays) != 1 || clock.delays[0] != 750*time.Millisecond {
		t.Fatalf("recorded delays = %v", clock.delays)
	}
}

func TestDeterministicClockSkipsImpossibleBackoff(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	clock := &deterministicClock{now: time.Now()}
	ctx, cancel := context.WithDeadline(context.Background(), clock.now.Add(100*time.Millisecond))
	defer cancel()
	client := NewClient(
		WithRetryMax(1),
		WithClock(clock),
		WithDelayFunc(func(int, error) time.Duration { return time.Second }),
	)
	resp, err := client.GetContext(ctx, server.URL)
	if resp != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got resp=%v err=%v", resp, err)
	}
	if len(clock.delays) != 0 {
		t.Fatalf("clock waits = %v, expected no wait", clock.delays)
	}
}

func TestDeterministicClockCancellationDuringBackoff(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	clock := &blockingClock{
		now:     time.Now(),
		started: make(chan struct{}),
		tick:    make(chan time.Time),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(
		WithRetryMax(1),
		WithClock(clock),
		WithDelayFunc(func(int, error) time.Duration { return time.Second }),
	)
	done := make(chan error, 1)
	go func() {
		_, err := client.GetContext(ctx, server.URL)
		done <- err
	}()
	select {
	case <-clock.started:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("retry did not reach deterministic clock")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("request did not stop after cancellation")
	}
}

func TestRetryResponseBodyIsClosedBeforeNextAttempt(t *testing.T) {
	firstBody := &trackedBody{reader: strings.NewReader("temporary")}
	transport := &failureInjectionTransport{attempts: []injectedAttempt{
		{response: &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Status:     "503 Service Unavailable",
			Header:     make(http.Header),
			Body:       firstBody,
		}},
		{response: injectedResponse(http.StatusOK, "ok")},
	}}
	client := NewClient(
		WithHTTPClient(&http.Client{Transport: transport}),
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
	)
	resp, err := client.Get("http://injected.test")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if !firstBody.closed.Load() {
		t.Fatal("retry response body was not closed")
	}
}

func TestClientConcurrentUse(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[string]int)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		mu.Lock()
		seen[id]++
		attempt := seen[id]
		mu.Unlock()
		if attempt == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(WithRetryMax(1), WithDelayFunc(func(int, error) time.Duration { return 0 }))
	const workers = 12
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodGet, server.URL, nil)
			if err == nil {
				req.Header.Set("X-Request-ID", fmt.Sprintf("request-%d", i))
				var resp *http.Response
				resp, err = client.Do(context.Background(), req)
				if resp != nil {
					resp.Body.Close()
				}
			}
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != workers {
		t.Fatalf("request IDs seen = %d, want %d", len(seen), workers)
	}
}

func TestConnectionReuseAfterRetryResponse(t *testing.T) {
	var requests atomic.Int32
	var newConnections atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("temporary failure"))
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConnections.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	transport := &http.Transport{}
	client := NewClient(
		WithHTTPClient(&http.Client{Transport: transport}),
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
	)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if newConnections.Load() != 1 {
		t.Fatalf("new connections = %d, want 1", newConnections.Load())
	}
}

func TestCompatibilityHooks(t *testing.T) {
	var calls atomic.Int32
	var requestHooks, responseHooks int
	logger := &testLogger{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(
		WithRetryMax(1),
		WithRetryWaitMin(time.Millisecond),
		WithRetryWaitMax(time.Second),
		WithBackoff(func(min, max time.Duration, attempt int, resp *http.Response) time.Duration {
			if min != time.Millisecond || max != time.Second || attempt != 1 || resp == nil || resp.StatusCode != http.StatusBadGateway {
				t.Errorf("backoff args = %s %s %d %v", min, max, attempt, resp)
			}
			return 0
		}),
		WithLogger(logger),
		WithRequestLogHook(func(got Logger, req *http.Request, attempt int) {
			requestHooks++
			if got != logger || req == nil {
				t.Fatal("request hook arguments are incorrect")
			}
		}),
		WithResponseLogHook(func(got Logger, resp *http.Response) {
			responseHooks++
			if got != logger || resp == nil {
				t.Fatal("response hook arguments are incorrect")
			}
		}),
	)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if requestHooks != 2 || responseHooks != 2 || len(logger.messages) != 0 {
		t.Fatalf("hooks: request=%d response=%d logs=%d", requestHooks, responseHooks, len(logger.messages))
	}
}

func TestLoggerConfiguration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	logger := &testLogger{}
	var requestLogger, responseLogger Logger
	client := NewClient(
		WithAttempts(1),
		WithLogger(logger),
		WithRequestLogHook(func(got Logger, req *http.Request, attempt int) {
			requestLogger = got
		}),
		WithResponseLogHook(func(got Logger, resp *http.Response) {
			responseLogger = got
		}),
	)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if requestLogger != logger || responseLogger != logger {
		t.Fatalf("logger values: request=%T response=%T", requestLogger, responseLogger)
	}

	client = NewClient(
		WithAttempts(1),
		WithLogger(nil),
		WithRequestLogHook(func(got Logger, req *http.Request, attempt int) {
			if got != nil {
				t.Fatalf("nil logger became %T", got)
			}
		}),
	)
	resp, err = client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestCheckRetryAndErrorHandler(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	checkCalls := 0
	handlerCalls := 0
	client := NewClient(
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
		WithCheckRetry(func(ctx context.Context, resp *http.Response, err error) (bool, error) {
			checkCalls++
			return true, nil
		}),
		WithErrorHandler(func(resp *http.Response, err error, tries int) (*http.Response, error) {
			handlerCalls++
			if resp == nil || tries != 2 || err == nil {
				t.Fatalf("handler args: resp=%v tries=%d err=%v", resp, tries, err)
			}
			return nil, err
		}),
	)
	resp, err := client.Get(server.URL)
	if err == nil || resp != nil {
		t.Fatalf("expected handled exhaustion, got resp=%v err=%v", resp, err)
	}
	if checkCalls != 2 || handlerCalls != 1 {
		t.Fatalf("check calls=%d handler calls=%d", checkCalls, handlerCalls)
	}
}

func TestCheckRetryTakesPrecedenceOverRetryIf(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
		WithRetryIf(func(*http.Response, error) bool { return false }),
		WithCheckRetry(func(_ context.Context, resp *http.Response, _ error) (bool, error) {
			return resp != nil && resp.StatusCode == http.StatusServiceUnavailable, nil
		}),
	)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2 because CheckRetry overrides RetryIf", calls.Load())
	}
}

func TestStatsErrorHandlerReceivesRetryStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	var received RetryStats
	client := NewClient(
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
		WithStatsErrorHandler(func(resp *http.Response, err error, tries int, stats RetryStats) (*http.Response, error) {
			if resp == nil || err == nil || tries != 2 {
				t.Fatalf("handler args: resp=%v err=%v tries=%d", resp, err, tries)
			}
			received = stats
			return nil, err
		}),
	)
	resp, err := client.Get(server.URL)
	if resp != nil || err == nil {
		t.Fatalf("expected exhausted retry error, got resp=%v err=%v", resp, err)
	}
	if received.Attempts != 2 || len(received.AttemptsDetail) != 2 {
		t.Fatalf("received stats = %+v", received)
	}
}

func TestMaxElapsedTimeStopsBeforeLongBackoff(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	started := time.Now()
	client := NewClient(
		WithRetryMax(1),
		WithMaxElapsedTime(50*time.Millisecond),
		WithDelayFunc(func(int, error) time.Duration { return time.Hour }),
	)
	resp, err := client.Get(server.URL)
	if resp != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error without response, got resp=%v err=%v", resp, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("elapsed time = %s, expected early termination", elapsed)
	}
}

func TestContextCancellationDuringBackoff(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(
		WithRetryMax(1),
		WithDelayFunc(func(int, error) time.Duration { return time.Hour }),
		WithOnRetry(func(AttemptInfo) { cancel() }),
	)
	resp, err := client.GetContext(ctx, server.URL)
	if resp != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation without response, got resp=%v err=%v", resp, err)
	}
}

func TestNewRequestBodyFactoryReplaysBody(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != "factory-body" {
			t.Errorf("body = %q", body)
		}
		if r.Header.Get("Idempotency-Key") != "request-123" {
			t.Errorf("idempotency key = %q", r.Header.Get("Idempotency-Key"))
		}
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	req, err := NewRequest(context.Background(), http.MethodPost, server.URL, func() (io.Reader, error) {
		return strings.NewReader("factory-body"), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Idempotency-Key", "request-123")
	clock := &deterministicClock{now: time.Now()}
	client := NewClient(WithAttempts(2), WithUnsafeMethods(), WithClock(clock), WithDelayFunc(func(int, error) time.Duration { return 100 * time.Millisecond }))
	resp, err := client.Do(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", calls.Load())
	}
	if len(clock.delays) != 1 || clock.delays[0] != 100*time.Millisecond {
		t.Fatalf("clock delays = %v", clock.delays)
	}
}

func TestRetryAfterIsCappedByMaxDelay(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	var info AttemptInfo
	client := NewClient(
		WithAttempts(2),
		WithMaxDelay(time.Millisecond),
		WithOnRetry(func(got AttemptInfo) { info = got }),
	)
	resp, err := client.Get(server.URL)
	if err == nil || resp != nil {
		t.Fatalf("expected exhausted retry error and no response, got resp=%v err=%v", resp, err)
	}
	if info.Delay > time.Millisecond {
		t.Fatalf("retry delay = %s, exceeds max delay", info.Delay)
	}
}

func TestDoWithStats(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(WithAttempts(2), WithDelayFunc(func(int, error) time.Duration { return 0 }))
	var observed []AttemptStats
	client.OnAttempt = func(info AttemptStats) { observed = append(observed, info) }
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, stats, err := client.DoWithStats(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if stats.Attempts != 2 || len(stats.Reasons) != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if len(stats.AttemptsDetail) != 2 || stats.AttemptsDetail[0].Duration <= 0 || stats.AttemptsDetail[1].Duration <= 0 {
		t.Fatalf("attempt details = %+v", stats.AttemptsDetail)
	}
	if len(observed) != 2 {
		t.Fatalf("observed attempts = %d, want 2", len(observed))
	}
	if stats.Reasons[0].StatusCode != http.StatusBadGateway {
		t.Fatalf("reason = %+v", stats.Reasons[0])
	}
	if stats.LastStatusCode != http.StatusOK {
		t.Fatalf("last status = %d", stats.LastStatusCode)
	}
}

func TestPrepareRetryReceivesPreviousResponse(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if r.Header.Get("X-Retry-Prepared") != "yes" {
			t.Errorf("retry preparation header = %q", r.Header.Get("X-Retry-Prepared"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(
		WithAttempts(2),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
		WithPrepareRetry(func(req *http.Request, resp *http.Response, err error) error {
			if resp == nil || resp.StatusCode != http.StatusServiceUnavailable || err == nil {
				t.Fatalf("previous attempt = resp=%v err=%v", resp, err)
			}
			var statusErr *attemptError
			if !errors.As(err, &statusErr) {
				t.Fatalf("previous error = %T %v, want retryable status error", err, err)
			}
			req.Header.Set("X-Retry-Prepared", "yes")
			return nil
		}),
	)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
}

func TestStandardClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer server.Close()
	client := NewClient(WithAttempts(1))
	resp, err := client.StandardClient().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
}

func TestRoundTripper(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(WithAttempts(1))
	standard := &http.Client{Transport: RoundTripper{Client: client}}
	resp, err := standard.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}

func TestConvenienceMethods(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			body, _ := io.ReadAll(r.Body)
			if r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" && string(body) != "a=1" {
				t.Errorf("post body = %q", body)
			}
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client := NewClient(WithAttempts(1))
	ctx := context.Background()
	for name, call := range map[string]func() (*http.Response, error){
		"get":      func() (*http.Response, error) { return client.Get(server.URL) },
		"get ctx":  func() (*http.Response, error) { return client.GetContext(ctx, server.URL) },
		"head":     func() (*http.Response, error) { return client.Head(server.URL) },
		"head ctx": func() (*http.Response, error) { return client.HeadContext(ctx, server.URL) },
		"post": func() (*http.Response, error) {
			return client.Post(server.URL, "text/plain", strings.NewReader("body"))
		},
		"post ctx": func() (*http.Response, error) {
			return client.PostContext(ctx, server.URL, "text/plain", strings.NewReader("body"))
		},
		"post form": func() (*http.Response, error) { return client.PostForm(server.URL, url.Values{"a": {"1"}}) },
		"form ctx":  func() (*http.Response, error) { return client.PostFormContext(ctx, server.URL, url.Values{"a": {"1"}}) },
	} {
		t.Run(name, func(t *testing.T) {
			resp, err := call()
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
		})
	}
}

func TestNewRequestBodyFormsAndErrors(t *testing.T) {
	ctx := context.Background()
	for name, body := range map[string]any{
		"nil":    nil,
		"bytes":  []byte("bytes"),
		"string": "string",
		"reader": strings.NewReader("reader"),
	} {
		t.Run(name, func(t *testing.T) {
			req, err := NewRequest(ctx, http.MethodPost, "http://example.test", body)
			if err != nil || req == nil {
				t.Fatalf("request=%v error=%v", req, err)
			}
		})
	}
	if _, err := NewRequest(nil, http.MethodGet, "http://example.test", nil); err == nil {
		t.Fatal("nil context should fail")
	}
	if _, err := NewRequest(ctx, http.MethodGet, "http://example.test", 123); err == nil {
		t.Fatal("unsupported body should fail")
	}
	if _, err := NewRequest(ctx, http.MethodGet, "://bad", nil); err == nil {
		t.Fatal("invalid URL should fail")
	}
	if _, err := NewRequest(ctx, http.MethodGet, "http://example.test", BodyFactory(func() (io.Reader, error) {
		return nil, errors.New("factory failed")
	})); err == nil {
		t.Fatal("factory error should fail")
	}
	req, err := NewRequest(ctx, http.MethodGet, "http://example.test", BodyFactory(func() (io.Reader, error) { return nil, nil }))
	if err != nil {
		t.Fatal(err)
	}
	body, err := req.GetBody()
	if err != nil || body == nil {
		t.Fatalf("nil factory body = %v, %v", body, err)
	}
	body.Close()
}

type closeErrorReader struct{}

func (closeErrorReader) Read([]byte) (int, error) { return 0, io.EOF }
func (closeErrorReader) Close() error             { return errors.New("close failed") }

type readErrorReader struct{}

func (readErrorReader) Read([]byte) (int, error) { return 0, errors.New("read failed") }
func (readErrorReader) Close() error             { return nil }

func TestMakeBodyFactoryBuffersAndReportsErrors(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "http://example.test", bytes.NewBufferString("body"))
	if err != nil {
		t.Fatal(err)
	}
	factory, err := makeBodyFactory(req)
	if err != nil || factory == nil {
		t.Fatalf("factory present=%t error=%v", factory != nil, err)
	}
	body, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(body)
	body.Close()
	if string(data) != "body" {
		t.Fatalf("body = %q", data)
	}

	for name, body := range map[string]io.ReadCloser{
		"read":  readErrorReader{},
		"close": closeErrorReader{},
	} {
		t.Run(name, func(t *testing.T) {
			req := &http.Request{Body: body}
			if _, err := makeBodyFactory(req); err == nil {
				t.Fatal("expected body buffering error")
			}
		})
	}
}

func TestDefaultPolicyAndRetryAfterEdges(t *testing.T) {
	if DefaultRetryPolicy(nil, nil) || DefaultRetryPolicy(&http.Response{StatusCode: http.StatusBadRequest}, nil) {
		t.Fatal("non-retryable results were retried")
	}
	if !DefaultRetryPolicy(nil, errors.New("network")) || DefaultRetryPolicy(nil, context.DeadlineExceeded) {
		t.Fatal("transport policy mismatch")
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for value, want := range map[string]time.Duration{
		"":           0,
		"0":          0,
		"-1":         0,
		"bad":        0,
		"9223372036": 9223372036 * time.Second,
	} {
		if got := retryAfter(value, now); got != want {
			t.Errorf("retryAfter(%q) = %s, want %s", value, got, want)
		}
	}
	if got := retryAfter(now.Add(2*time.Second).Format(http.TimeFormat), now); got != 2*time.Second {
		t.Fatalf("HTTP date delay = %s", got)
	}
	if retryAfter(now.Add(-time.Second).Format(http.TimeFormat), now) != 0 {
		t.Fatal("past HTTP date should not delay")
	}
}

func TestRetryAfterErrorAndHelpers(t *testing.T) {
	resp := injectedResponse(http.StatusTooManyRequests, "")
	resp.Header.Set("Retry-After", "2")
	err := newResponseError(resp)
	var retryErr interface{ RetryAfter() time.Duration }
	if !errors.As(err, &retryErr) || retryErr.RetryAfter() <= 0 {
		t.Fatalf("retry-after error = %T %v", err, err)
	}
	if (&attemptError{err: errors.New("transport")}).Error() != "transport" {
		t.Fatal("transport error message mismatch")
	}
	if (&attemptError{response: resp}).Error() == "" {
		t.Fatal("response error message is empty")
	}
	if statusCode(nil) != 0 || statusCode(resp) != resp.StatusCode {
		t.Fatal("statusCode mismatch")
	}
	drainAndClose(nil)
	drainAndClose(&http.Response{})
}

func TestDoValidationAndRoundTripperErrors(t *testing.T) {
	client := NewClient()
	if _, _, err := client.DoWithStats(context.Background(), nil); err == nil {
		t.Fatal("nil request should fail")
	}
	req, _ := http.NewRequest(http.MethodGet, "http://example.test", nil)
	if _, _, err := client.DoWithStats(nil, req); err == nil {
		t.Fatal("nil context should fail")
	}
	if _, err := (RoundTripper{}).RoundTrip(req); err == nil {
		t.Fatal("nil round tripper client should fail")
	}
}

func TestOptionCoverage(t *testing.T) {
	client := NewClient(
		WithHTTPClient(nil), WithTryOptions(), WithRetryMax(-1), WithInitialDelay(time.Millisecond),
		WithClock(nil),
		WithRetryIf(func(*http.Response, error) bool { return false }), WithOnRetry(nil), WithOnAttempt(nil),
		WithPrepareRetry(nil), WithCheckRetry(nil), WithBackoff(nil), WithErrorHandler(nil),
		WithStatsErrorHandler(nil), WithRequestLogHook(nil), WithResponseLogHook(nil), WithMaxElapsedTime(0),
	)
	if client.RetryIf == nil || client.HTTPClient == nil {
		t.Fatal("options corrupted client defaults")
	}
}

func TestDeadlineAwareClockDelegatesAndSkips(t *testing.T) {
	base := &deterministicClock{now: time.Now()}
	ctx := context.Background()
	clock := deadlineAwareClock{base: base, ctx: ctx}
	if clock.Now() != base.now {
		t.Fatalf("clock now = %v, want %v", clock.Now(), base.now)
	}
	select {
	case <-clock.After(time.Millisecond):
	case <-time.After(time.Second):
		t.Fatal("delegated clock did not fire")
	}
	if len(base.delays) != 1 || base.delays[0] != time.Millisecond {
		t.Fatalf("delegated delays = %v", base.delays)
	}

	deadlineCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Millisecond))
	defer cancel()
	base.delays = nil
	clock = deadlineAwareClock{base: base, ctx: deadlineCtx}
	select {
	case <-clock.After(time.Hour):
	case <-time.After(time.Second):
		t.Fatal("skipped clock did not fire")
	}
	if len(base.delays) != 0 {
		t.Fatalf("skipped delay delegated to base clock: %v", base.delays)
	}
}

func TestRealClock(t *testing.T) {
	clock := realClock{}
	if clock.Now().IsZero() {
		t.Fatal("real clock returned zero time")
	}
	select {
	case <-clock.After(0):
	case <-time.After(time.Second):
		t.Fatal("real clock did not fire")
	}
}

func TestErrorHandlerCanReturnResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	customErr := errors.New("custom final error")
	client := NewClient(
		WithAttempts(1),
		WithErrorHandler(func(resp *http.Response, err error, tries int) (*http.Response, error) {
			if resp == nil || err == nil || tries != 1 {
				t.Fatalf("handler args: response=%v error=%v tries=%d", resp, err, tries)
			}
			return resp, customErr
		}),
	)
	resp, err := client.Get(server.URL)
	if resp == nil || !errors.Is(err, customErr) {
		t.Fatalf("response=%v error=%v", resp, err)
	}
	resp.Body.Close()
}

func TestPrepareRetryErrorStopsBeforeNextAttempt(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	prepareErr := errors.New("refresh credentials failed")
	client := NewClient(
		WithAttempts(2),
		WithDelayFunc(func(int, error) time.Duration { return 0 }),
		WithPrepareRetry(func(*http.Request, *http.Response, error) error { return prepareErr }),
	)
	resp, err := client.Get(server.URL)
	if resp != nil || !errors.Is(err, prepareErr) {
		t.Fatalf("response=%v error=%v", resp, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("server calls = %d, want 1", calls.Load())
	}
}

func TestCheckRetryErrorStopsStatusRetry(t *testing.T) {
	transport := &failureInjectionTransport{attempts: []injectedAttempt{{response: injectedResponse(http.StatusServiceUnavailable, "")}}}
	checkErr := errors.New("retry classification failed")
	client := NewClient(
		WithHTTPClient(&http.Client{Transport: transport}),
		WithAttempts(2),
		WithCheckRetry(func(context.Context, *http.Response, error) (bool, error) { return false, checkErr }),
	)
	resp, err := client.Get("http://injected.test")
	if resp != nil || !errors.Is(err, checkErr) {
		t.Fatalf("response=%v error=%v", resp, err)
	}
	if transport.calls.Load() != 1 {
		t.Fatalf("transport calls = %d, want 1", transport.calls.Load())
	}
}

func TestCheckRetryErrorStopsTransportRetry(t *testing.T) {
	transport := &failureInjectionTransport{attempts: []injectedAttempt{{err: errors.New("connection reset")}}}
	checkErr := errors.New("retry classification failed")
	client := NewClient(
		WithHTTPClient(&http.Client{Transport: transport}),
		WithAttempts(2),
		WithCheckRetry(func(context.Context, *http.Response, error) (bool, error) { return false, checkErr }),
	)
	resp, err := client.Get("http://injected.test")
	if resp != nil || !errors.Is(err, checkErr) {
		t.Fatalf("response=%v error=%v", resp, err)
	}
}

func TestNilHTTPClientAndRetryPolicyUseDefaults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewClient(WithAttempts(1))
	client.HTTPClient = nil
	client.RetryIf = nil
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
}
