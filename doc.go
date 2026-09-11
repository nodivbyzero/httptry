// Package httptry adds retry-aware HTTP requests on top of the generic
// github.com/nodivbyzero/try retry engine.
//
// # Basic usage
//
// Create a client with [NewClient] and use its net/http-style convenience
// methods:
//
//	client := httptry.NewClient(httptry.WithRetryMax(3))
//	resp, err := client.Get("https://api.example.com/health")
//	if err != nil {
//		return err
//	}
//	defer resp.Body.Close()
//
// For request-scoped cancellation, construct a request with [NewRequest] and
// call [Client.Do]:
//
//	req, err := httptry.NewRequest(ctx, http.MethodGet, url, nil)
//	if err != nil {
//		return err
//	}
//	resp, err := client.Do(ctx, req)
//
// # Default retry policy
//
// The default policy retries transport errors, 408, 429, and 5xx responses
// except 501. Context cancellation and deadline errors are not retried.
// Retry-After headers containing seconds or HTTP dates are parsed and bounded
// by the configured maximum delay.
//
// Retries are enabled by default for idempotent methods: GET, HEAD, OPTIONS,
// PUT, DELETE, and TRACE. POST and PATCH require [WithUnsafeMethods], because
// repeating them can duplicate side effects.
//
// # Request body replay
//
// [NewRequest] accepts strings, byte slices, io.Readers, and [BodyFactory]
// values. A body factory creates a fresh reader for the initial request and
// every retry. If a request has no GetBody function, Client buffers the body
// before the first attempt so it can be replayed. Large or streaming bodies
// should provide an explicit factory.
//
// # Configuration
//
// Functional options configure attempt counts, delays, retry policies, custom
// transports, elapsed-time limits, clocks, unsafe methods, and compatibility
// hooks. [WithRetryMax] counts retries after the initial attempt, while
// [WithAttempts] counts the initial attempt itself.
// [WithAttemptTimeout] limits each individual HTTP attempt independently of
// [WithMaxElapsedTime].
//
// [WithMaxElapsedTime] bounds the complete operation, including HTTP attempts
// and backoff. Backoff is also checked against the request context deadline so
// an impossible sleep is not started.
//
// # Observability
//
// [Client.DoWithStats] returns [RetryStats], including total duration, backoff
// duration, final status, retry reasons, and per-attempt [AttemptStats].
// [WithOnAttempt] observes each completed attempt, while [WithOnRetry] observes
// each retry before its backoff delay. [WithStatsErrorHandler] exposes complete
// statistics when retries are exhausted.
//
// [WithPrepareRetry] receives the previous response and error before the next
// attempt. It can refresh credentials, change query parameters, or otherwise
// prepare the request.
// For retryable status responses, the error is a retryable status error; for
// transport and response-header failures, it is the underlying failure. The
// previous response has already been drained and closed.
//
// # Compatibility and transports
//
// Compatibility options include [WithCheckRetry], [WithBackoff],
// [WithErrorHandler], [WithRequestLogHook], [WithResponseLogHook],
// [WithLogger], [WithRetryWaitMin], and [WithRetryWaitMax]. These map common
// go-retryablehttp concepts to the functional-option API.
// If both [WithRetryIf] and [WithCheckRetry] are configured, CheckRetry takes
// precedence and its boolean decision replaces RetryIf's decision.
//
// [Client.StandardClient] returns a standard-library client with retries. The
// exported [RoundTripper] can be composed directly with a custom http.Client
// when callers need settings such as Timeout, Jar, or CheckRedirect.
//
// # Response bodies and cancellation
//
// Client retries failures while sending the request or receiving response
// headers. It returns successful and non-retryable response bodies to the
// caller, who owns and must close them. Retryable response bodies are drained
// and closed before the next attempt. A body read failure that occurs after
// Client.Do returns is caller-managed and is not automatically retried.
// Context cancellation and deadline expiry are normal control flow; httptry
// does not emit built-in error-level logs for them.
// A request must not be shared between concurrent [Client.Do] calls. Retry
// preparation hooks receive and may mutate the original request.
package httptry
