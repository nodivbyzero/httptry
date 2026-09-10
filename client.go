package httptry

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nodivbyzero/try"
)

// RetryPolicy decides whether an HTTP attempt should be retried.
// resp is the response returned by the server, if any; err is the transport
// or response-processing error, if any.
type RetryPolicy func(resp *http.Response, err error) bool

// AttemptInfo describes a failed attempt for observability hooks.
type AttemptInfo struct {
	Attempt  int
	Response *http.Response
	Err      error
	Delay    time.Duration
}

// RetryReason records why an attempt was retried.
type RetryReason struct {
	Attempt    int
	StatusCode int
	Err        error
	Delay      time.Duration
}

// AttemptStats records the result and duration of one HTTP attempt.
type AttemptStats struct {
	Attempt    int
	Duration   time.Duration
	StatusCode int
	Err        error
}

// AttemptHook observes every completed HTTP attempt.
type AttemptHook func(AttemptStats)

// RetryStats contains timing and outcome information for one Do operation.
type RetryStats struct {
	Attempts        int
	TotalDuration   time.Duration
	BackoffDuration time.Duration
	LastStatusCode  int
	Reasons         []RetryReason
	AttemptsDetail  []AttemptStats
}

// Option configures a Client.
type Option func(*Client)

// BodyFactory creates a fresh request body for every attempt.
type BodyFactory func() (io.Reader, error)

// PrepareRetry updates req before a retry. resp and err describe the
// immediately preceding attempt. The hook can refresh authentication,
// adjust query parameters, or otherwise prepare the next request.
type PrepareRetry func(req *http.Request, resp *http.Response, err error) error

// CheckRetry is the compatibility form of a retry policy. Returning an error
// stops retries and makes that error the operation error. When both
// CheckRetry and RetryIf are configured, CheckRetry takes precedence.
type CheckRetry func(ctx context.Context, resp *http.Response, err error) (bool, error)

// Backoff calculates the delay before the next retry.
type Backoff func(min, max time.Duration, attempt int, resp *http.Response) time.Duration

// ErrorHandler handles the final exhausted-retry result.
type ErrorHandler func(resp *http.Response, err error, numTries int) (*http.Response, error)

// StatsErrorHandler handles the final result with full retry statistics.
type StatsErrorHandler func(resp *http.Response, err error, numTries int, stats RetryStats) (*http.Response, error)

// Logger is the small logging interface used by compatibility hooks.
type Logger interface {
	Printf(format string, args ...interface{})
}

// RequestLogHook runs before each attempt. attempt is zero-based.
type RequestLogHook func(logger Logger, req *http.Request, attempt int)

// ResponseLogHook runs after each attempt that returns a response.
type ResponseLogHook func(logger Logger, resp *http.Response)

// Client performs HTTP requests with retry support. A Client is safe for
// concurrent use after construction.
type Client struct {
	HTTPClient         *http.Client
	Options            []try.Option
	RetryIf            RetryPolicy
	OnRetry            func(AttemptInfo)
	OnAttempt          AttemptHook
	PrepareRetry       PrepareRetry
	CheckRetry         CheckRetry
	Backoff            Backoff
	ErrorHandler       ErrorHandler
	StatsErrorHandler  StatsErrorHandler
	RequestLogHook     RequestLogHook
	ResponseLogHook    ResponseLogHook
	Logger             Logger
	retryWaitMin       time.Duration
	retryWaitMax       time.Duration
	MaxElapsedTime     time.Duration
	retryUnsafeMethods bool
	clock              try.Clock
}

// NewClient returns a Client using the standard HTTP transport and conservative
// retries for idempotent methods.
func NewClient(opts ...Option) *Client {
	c := &Client{
		HTTPClient:   http.DefaultClient,
		RetryIf:      DefaultRetryPolicy,
		retryWaitMin: 200 * time.Millisecond,
		retryWaitMax: 30 * time.Second,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// NewRequest creates an HTTP request with a replayable body. In addition to
// the body types accepted by net/http, body may be a BodyFactory. A factory is
// called once to create the initial body and again before each retry.
func NewRequest(ctx context.Context, method, url string, body any) (*http.Request, error) {
	if ctx == nil {
		return nil, errors.New("httptry: nil context")
	}
	if body == nil {
		return http.NewRequestWithContext(ctx, method, url, nil)
	}

	if factory, ok := body.(BodyFactory); ok {
		reader, err := factory()
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, url, reader)
		if err != nil {
			return nil, err
		}
		req.GetBody = func() (io.ReadCloser, error) {
			next, err := factory()
			if err != nil {
				return nil, err
			}
			if next == nil {
				return http.NoBody, nil
			}
			return io.NopCloser(next), nil
		}
		return req, nil
	}

	if factory, ok := body.(func() (io.Reader, error)); ok {
		return NewRequest(ctx, method, url, BodyFactory(factory))
	}

	var reader io.Reader
	switch value := body.(type) {
	case []byte:
		reader = bytes.NewReader(value)
	case string:
		reader = strings.NewReader(value)
	case io.Reader:
		reader = value
	default:
		return nil, fmt.Errorf("httptry: unsupported request body type %T", body)
	}
	return http.NewRequestWithContext(ctx, method, url, reader)
}

// WithHTTPClient sets the underlying HTTP client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.HTTPClient = hc
		}
	}
}

// WithTryOptions appends options passed to the generic retry engine.
func WithTryOptions(opts ...try.Option) Option {
	return func(c *Client) { c.Options = append(c.Options, opts...) }
}

// WithAttempts sets the maximum number of attempts, including the first.
func WithAttempts(n int) Option { return WithTryOptions(try.WithAttempts(n)) }

// WithRetryMax sets the maximum number of retries after the initial attempt.
// It mirrors retryablehttp's RetryMax naming; WithAttempts counts the initial
// attempt as well.
func WithRetryMax(n int) Option {
	if n < 0 {
		n = 0
	}
	return WithAttempts(n + 1)
}

// WithInitialDelay sets the initial backoff delay.
func WithInitialDelay(d time.Duration) Option { return WithTryOptions(try.WithInitialDelay(d)) }

// WithRetryWaitMin sets the lower bound passed to a compatibility Backoff.
func WithRetryWaitMin(d time.Duration) Option {
	return func(c *Client) {
		c.retryWaitMin = d
		c.Options = append(c.Options, try.WithInitialDelay(d))
	}
}

// WithMaxDelay sets the maximum single backoff delay.
func WithMaxDelay(d time.Duration) Option { return WithTryOptions(try.WithMaxDelay(d)) }

// WithRetryWaitMax sets the upper bound passed to a compatibility Backoff.
func WithRetryWaitMax(d time.Duration) Option {
	return func(c *Client) {
		c.retryWaitMax = d
		c.Options = append(c.Options, try.WithMaxDelay(d))
	}
}

// WithDelayFunc replaces the generic engine's backoff calculation.
func WithDelayFunc(fn func(attempt int, err error) time.Duration) Option {
	return WithTryOptions(try.WithDelayFunc(fn))
}

// WithClock supplies a clock for deterministic tests.
func WithClock(clock try.Clock) Option {
	return func(c *Client) {
		if clock != nil {
			c.clock = clock
			c.Options = append(c.Options, try.WithClock(clock))
		}
	}
}

// WithRetryIf replaces the default HTTP retry policy. It is ignored for an
// attempt when CheckRetry is configured, because CheckRetry takes precedence.
func WithRetryIf(policy RetryPolicy) Option {
	return func(c *Client) {
		if policy != nil {
			c.RetryIf = policy
		}
	}
}

// WithOnRetry registers a callback before each retry wait.
func WithOnRetry(fn func(AttemptInfo)) Option {
	return func(c *Client) { c.OnRetry = fn }
}

// WithOnAttempt registers a callback after every completed HTTP attempt.
func WithOnAttempt(fn AttemptHook) Option {
	return func(c *Client) { c.OnAttempt = fn }
}

// WithPrepareRetry registers a hook invoked before every retry attempt.
func WithPrepareRetry(fn PrepareRetry) Option {
	return func(c *Client) { c.PrepareRetry = fn }
}

// WithCheckRetry sets a retryablehttp-compatible retry callback.
func WithCheckRetry(fn CheckRetry) Option {
	return func(c *Client) { c.CheckRetry = fn }
}

// WithBackoff sets a retryablehttp-compatible backoff callback.
func WithBackoff(fn Backoff) Option {
	return func(c *Client) { c.Backoff = fn }
}

// WithErrorHandler sets a callback for the final operation result.
func WithErrorHandler(fn ErrorHandler) Option {
	return func(c *Client) { c.ErrorHandler = fn }
}

// WithStatsErrorHandler sets an error callback that also receives RetryStats.
func WithStatsErrorHandler(fn StatsErrorHandler) Option {
	return func(c *Client) { c.StatsErrorHandler = fn }
}

// WithRequestLogHook sets a callback before each attempt.
func WithRequestLogHook(fn RequestLogHook) Option {
	return func(c *Client) { c.RequestLogHook = fn }
}

// WithResponseLogHook sets a callback after each response.
func WithResponseLogHook(fn ResponseLogHook) Option {
	return func(c *Client) { c.ResponseLogHook = fn }
}

// WithLogger sets the logger passed to compatibility hooks.
func WithLogger(logger Logger) Option {
	return func(c *Client) { c.Logger = logger }
}

// WithMaxElapsedTime bounds the complete operation, including attempts and
// backoff. The request context deadline still takes precedence.
func WithMaxElapsedTime(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.MaxElapsedTime = d
		}
	}
}

// WithUnsafeMethods allows retries for methods such as POST and PATCH when
// their bodies are replayable. It should normally be paired with an idempotency
// key or an application-level guarantee that repeating the operation is safe.
func WithUnsafeMethods() Option {
	return func(c *Client) { c.retryUnsafeMethods = true }
}

// DefaultRetryPolicy retries transport errors and 408, 429, and 5xx responses,
// except 501. Context cancellation and deadline errors are never retried.
func DefaultRetryPolicy(resp *http.Response, err error) bool {
	if err != nil {
		return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
	}
	if resp == nil {
		return false
	}
	return resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode == http.StatusTooManyRequests ||
		(resp.StatusCode >= 500 && resp.StatusCode != http.StatusNotImplemented)
}

// Do executes req, retrying according to the Client configuration. A response
// is returned for successful or non-retryable HTTP results. When retries are
// exhausted, the final retryable response body is drained and closed and the
// response is not returned.
func (c *Client) Do(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, _, err := c.DoWithStats(ctx, req)
	return resp, err
}

// DoWithStats executes req and returns structured information about the
// attempts and waits used by the operation.
func (c *Client) DoWithStats(ctx context.Context, req *http.Request) (*http.Response, RetryStats, error) {
	started := time.Now()
	stats := RetryStats{}
	recordAttempt := func(detail AttemptStats) {
		stats.AttemptsDetail = append(stats.AttemptsDetail, detail)
		if c.OnAttempt != nil {
			c.OnAttempt(detail)
		}
	}
	finish := func(resp *http.Response, err error) (*http.Response, RetryStats, error) {
		stats.TotalDuration = time.Since(started)
		if resp != nil {
			stats.LastStatusCode = resp.StatusCode
		}
		return resp, stats, err
	}

	if req == nil {
		return finish(nil, errors.New("httptry: nil request"))
	}
	if ctx == nil {
		return finish(nil, errors.New("httptry: nil context"))
	}
	hc := c.HTTPClient
	if hc == nil {
		hc = http.DefaultClient
	}

	bodyFactory, err := makeBodyFactory(req)
	if err != nil {
		return finish(nil, err)
	}

	if !c.retryUnsafeMethods && !safeMethod(req.Method) {
		// Preserve the normal request behavior for unsafe methods unless the
		// caller explicitly opts into retrying them.
		attemptStarted := time.Now()
		resp, err := c.doOnce(ctx, hc, req, bodyFactory)
		stats.Attempts = 1
		recordAttempt(AttemptStats{
			Attempt:    1,
			Duration:   time.Since(attemptStarted),
			StatusCode: statusCode(resp),
			Err:        err,
		})
		return finish(resp, err)
	}

	operationCtx := ctx
	var cancel context.CancelFunc
	if c.MaxElapsedTime > 0 {
		operationCtx, cancel = context.WithTimeout(ctx, c.MaxElapsedTime)
	} else {
		// Keep a private cancellation path so the HTTP adapter can stop an
		// impossible backoff even when the published try engine predates its
		// deadline-aware wait logic.
		operationCtx, cancel = context.WithCancel(ctx)
	}
	defer cancel()

	policy := c.RetryIf
	if policy == nil {
		policy = DefaultRetryPolicy
	}
	deadlineExceededDuringBackoff := false
	options := append([]try.Option(nil), c.Options...)
	baseClock := c.clock
	if baseClock == nil {
		baseClock = realClock{}
	}
	options = append(options, try.WithClock(deadlineAwareClock{base: baseClock, ctx: operationCtx}))
	if c.Backoff != nil {
		options = append(options, try.WithDelayFunc(func(attempt int, err error) time.Duration {
			var ae *attemptError
			if errors.As(err, &ae) {
				return c.Backoff(c.retryWaitMin, c.retryWaitMax, attempt, ae.response)
			}
			return c.Backoff(c.retryWaitMin, c.retryWaitMax, attempt, nil)
		}))
	}
	options = append(options, try.WithRetryIf(func(err error) bool {
		var ae *attemptError
		if !errors.As(err, &ae) {
			return false
		}
		if ae.retryDecisionSet {
			return ae.shouldRetry
		}
		return policy(ae.response, ae.err)
	}))
	if c.OnRetry != nil {
		options = append(options, try.WithOnRetry(func(info try.RetryInfo) {
			if deadline, ok := operationCtx.Deadline(); ok && info.Delay > time.Until(deadline) {
				deadlineExceededDuringBackoff = true
				cancel()
			}
			var ae *attemptError
			if errors.As(info.Err, &ae) {
				stats.BackoffDuration += info.Delay
				stats.Reasons = append(stats.Reasons, RetryReason{
					Attempt:    info.Attempt,
					StatusCode: statusCode(ae.response),
					Err:        ae.err,
					Delay:      info.Delay,
				})
				c.OnRetry(AttemptInfo{Attempt: info.Attempt, Response: ae.response, Err: ae.err, Delay: info.Delay})
			}
		}))
	} else {
		options = append(options, try.WithOnRetry(func(info try.RetryInfo) {
			if deadline, ok := operationCtx.Deadline(); ok && info.Delay > time.Until(deadline) {
				deadlineExceededDuringBackoff = true
				cancel()
			}
			var ae *attemptError
			if errors.As(info.Err, &ae) {
				stats.BackoffDuration += info.Delay
				stats.Reasons = append(stats.Reasons, RetryReason{
					Attempt:    info.Attempt,
					StatusCode: statusCode(ae.response),
					Err:        ae.err,
					Delay:      info.Delay,
				})
			}
		}))
	}

	var lastResponse *http.Response
	var previousResponse *http.Response
	var previousErr error
	_, err = try.Do(operationCtx, func(attemptCtx context.Context) (*http.Response, error) {
		stats.Attempts++
		if stats.Attempts > 1 && c.PrepareRetry != nil {
			if prepareErr := c.PrepareRetry(req, previousResponse, previousErr); prepareErr != nil {
				return nil, try.Permanent(&attemptError{err: prepareErr})
			}
		}
		if c.RequestLogHook != nil {
			c.RequestLogHook(c.Logger, req, stats.Attempts-1)
		}
		attemptStarted := time.Now()
		response, requestErr := c.doOnce(attemptCtx, hc, req, bodyFactory)
		lastResponse = response
		previousResponse = response
		previousErr = requestErr
		if response != nil {
			stats.LastStatusCode = response.StatusCode
			if c.ResponseLogHook != nil {
				c.ResponseLogHook(c.Logger, response)
			}
		}
		if requestErr != nil {
			recordAttempt(AttemptStats{
				Attempt:    stats.Attempts,
				Duration:   time.Since(attemptStarted),
				StatusCode: statusCode(response),
				Err:        requestErr,
			})
			if c.CheckRetry != nil {
				shouldRetry, checkErr := c.CheckRetry(attemptCtx, response, requestErr)
				if checkErr != nil {
					return nil, try.Permanent(&attemptError{response: response, err: checkErr})
				}
				if !shouldRetry {
					return nil, try.Permanent(&attemptError{response: response, err: requestErr})
				}
				return response, &attemptError{
					err:              requestErr,
					response:         response,
					retryDecisionSet: true,
					shouldRetry:      true,
				}
			}
			return response, &attemptError{err: requestErr, response: response}
		}
		shouldRetry := policy(response, nil)
		if c.CheckRetry != nil {
			var checkErr error
			shouldRetry, checkErr = c.CheckRetry(attemptCtx, response, nil)
			if checkErr != nil {
				return nil, try.Permanent(&attemptError{response: response, err: checkErr})
			}
			if !shouldRetry {
				recordAttempt(AttemptStats{
					Attempt:    stats.Attempts,
					Duration:   time.Since(attemptStarted),
					StatusCode: statusCode(response),
				})
				return response, nil
			}
		}
		if !shouldRetry {
			recordAttempt(AttemptStats{
				Attempt:    stats.Attempts,
				Duration:   time.Since(attemptStarted),
				StatusCode: statusCode(response),
			})
			return response, nil
		}
		recordAttempt(AttemptStats{
			Attempt:    stats.Attempts,
			Duration:   time.Since(attemptStarted),
			StatusCode: statusCode(response),
		})
		drainAndClose(response)
		responseErr := newResponseError(response)
		if c.CheckRetry != nil {
			if ae, ok := responseErr.(*attemptError); ok {
				ae.retryDecisionSet = true
				ae.shouldRetry = true
			} else if ae, ok := responseErr.(*retryAfterAttemptError); ok {
				ae.retryDecisionSet = true
				ae.shouldRetry = true
			}
		}
		previousErr = responseErr
		return response, responseErr
	}, options...)
	if err == nil {
		return finish(lastResponse, nil)
	}
	if deadlineExceededDuringBackoff {
		err = context.DeadlineExceeded
	}
	if c.ErrorHandler != nil {
		response, handledErr := c.ErrorHandler(lastResponse, err, stats.Attempts)
		return finish(response, handledErr)
	}
	if c.StatsErrorHandler != nil {
		response, handledErr := c.StatsErrorHandler(lastResponse, err, stats.Attempts, stats)
		return finish(response, handledErr)
	}
	return finish(nil, err)
}

// StandardClient returns a standard-library client whose transport performs
// retries through c.
func (c *Client) StandardClient() *http.Client {
	return &http.Client{Transport: RoundTripper{Client: c}}
}

// Get performs a GET request with a background context, matching
// net/http.Client.Get and retryablehttp.Client.Get.
func (c *Client) Get(url string) (*http.Response, error) {
	return c.GetContext(context.Background(), url)
}

// GetContext performs a GET request with ctx.
func (c *Client) GetContext(ctx context.Context, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(ctx, req)
}

// Head performs a HEAD request with a background context.
func (c *Client) Head(url string) (*http.Response, error) {
	return c.HeadContext(context.Background(), url)
}

// HeadContext performs a HEAD request with ctx.
func (c *Client) HeadContext(ctx context.Context, target string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, target, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(ctx, req)
}

// Post performs a POST request with a background context.
func (c *Client) Post(target, contentType string, body io.Reader) (*http.Response, error) {
	return c.PostContext(context.Background(), target, contentType, body)
}

// PostContext performs a POST request with ctx.
func (c *Client) PostContext(ctx context.Context, target, contentType string, body io.Reader) (*http.Response, error) {
	req, err := NewRequest(ctx, http.MethodPost, target, body)
	if err != nil {
		return nil, err
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.Do(ctx, req)
}

// PostForm submits form values with a background context.
func (c *Client) PostForm(target string, data url.Values) (*http.Response, error) {
	return c.Post(target, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
}

// PostFormContext submits form values with ctx.
func (c *Client) PostFormContext(ctx context.Context, target string, data url.Values) (*http.Response, error) {
	return c.PostContext(ctx, target, "application/x-www-form-urlencoded", strings.NewReader(data.Encode()))
}

func (c *Client) doOnce(ctx context.Context, hc *http.Client, original *http.Request, bodyFactory func() (io.ReadCloser, error)) (*http.Response, error) {
	req := original.Clone(ctx)
	if bodyFactory != nil {
		body, err := bodyFactory()
		if err != nil {
			return nil, err
		}
		req.Body = body
	}
	return hc.Do(req)
}

func makeBodyFactory(req *http.Request) (func() (io.ReadCloser, error), error) {
	if req.Body == nil {
		return nil, nil
	}
	if req.GetBody != nil {
		return req.GetBody, nil
	}
	data, err := io.ReadAll(req.Body)
	closeErr := req.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("httptry: buffer request body: %w", err)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("httptry: close request body: %w", closeErr)
	}
	return func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}, nil
}

func safeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodPut, http.MethodDelete, http.MethodTrace:
		return true
	default:
		return false
	}
}

type attemptError struct {
	response         *http.Response
	err              error
	retryDecisionSet bool
	shouldRetry      bool
}

type realClock struct{}

func (realClock) Now() time.Time                             { return time.Now() }
func (realClock) After(delay time.Duration) <-chan time.Time { return time.After(delay) }

type deadlineAwareClock struct {
	base try.Clock
	ctx  context.Context
}

func (c deadlineAwareClock) Now() time.Time { return c.base.Now() }

func (c deadlineAwareClock) After(delay time.Duration) <-chan time.Time {
	if deadline, ok := c.ctx.Deadline(); ok && delay > time.Until(deadline) {
		tick := make(chan time.Time, 1)
		tick <- time.Now()
		return tick
	}
	return c.base.After(delay)
}

func (e *attemptError) Error() string {
	if e.response != nil {
		return fmt.Sprintf("httptry: retryable HTTP status %d", e.response.StatusCode)
	}
	return e.err.Error()
}

func (e *attemptError) Unwrap() error { return e.err }

type retryAfterAttemptError struct {
	*attemptError
	delay time.Duration
}

func (e *retryAfterAttemptError) RetryAfter() time.Duration { return e.delay }

func newResponseError(resp *http.Response) error {
	e := &attemptError{response: resp}
	if resp != nil {
		if delay := retryAfter(resp.Header.Get("Retry-After"), time.Now()); delay > 0 {
			return &retryAfterAttemptError{attemptError: e, delay: delay}
		}
	}
	return e
}

func retryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil {
		if seconds > 0 {
			// Return the largest representable duration on overflow. The generic
			// retry engine will apply the configured MaxDelay cap.
			if seconds > int64(time.Duration(1<<63-1)/time.Second) {
				return time.Duration(1<<63 - 1)
			}
			return time.Duration(seconds) * time.Second
		}
		return 0
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

func drainAndClose(resp *http.Response) {
	if resp == nil || resp.Body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
}

func statusCode(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}

// RoundTripper adapts a Client to net/http.RoundTripper. It can be composed
// with a custom *http.Client directly when callers need standard client
// settings such as Timeout, CheckRedirect, or Jar.
type RoundTripper struct {
	Client *Client
}

func (r RoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if r.Client == nil {
		return nil, errors.New("httptry: nil RoundTripper client")
	}
	return r.Client.Do(req.Context(), req)
}
