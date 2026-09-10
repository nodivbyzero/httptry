# httptry

[![Go Documentation](http://img.shields.io/badge/go-documentation-blue.svg?style=flat-square)][godocs]

[godocs]: https://pkg.go.dev/github.com/nodivbyzero/httptry

Full API docs: [pkg.go.dev/github.com/nodivbyzero/httptry](https://pkg.go.dev/github.com/nodivbyzero/httptry/)


Retry-aware HTTP requests built on top of the generic
[`github.com/nodivbyzero/try`](https://github.com/nodivbyzero/try) retry engine.

`httptry` provides familiar `net/http`-style usage while adding replayable
request bodies, retryable HTTP status handling, bounded backoff, retry
statistics, and hooks for preparing and observing attempts.

## Where `httptry` is stronger

Compared with `go-retryablehttp`, `httptry` already provides several
deliberate safety and observability improvements:

- Safe methods are retried by default; `POST` and `PATCH` require explicit
  `WithUnsafeMethods`, reducing accidental duplication of non-idempotent work.
- `DoWithStats` and `RetryStats` provide built-in attempt counts, durations,
  backoff totals, status codes, and retry reasons.
- `WithPrepareRetry` can refresh credentials or re-sign a request between
  attempts.
- `WithMaxElapsedTime` bounds the complete operation independently of retry
  count.
- `Retry-After` accepts seconds and HTTP dates for any retryable response and
  is capped by the configured maximum delay.

These are intentional behavior differences; review retry safety before
changing an existing production policy.

## Requirements

- Go 1.27+

## Install

```bash
go get github.com/nodivbyzero/httptry
```

## Quick start

```go
package main

import (
	"io"

	"github.com/nodivbyzero/httptry"
)

func main() {
	client := httptry.NewClient(
		httptry.WithRetryMax(3),
	)

	resp, err := client.Get("https://api.example.com/health")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	_ = body
}
```

For requests that need a context, use `Do` or a context-specific convenience
method:

```go
req, err := httptry.NewRequest(ctx, http.MethodGet, url, nil)
if err != nil {
	return err
}

resp, err := client.Do(ctx, req)
```

## Default behavior

The default policy retries:

- transport/network errors;
- `408 Request Timeout`;
- `429 Too Many Requests`;
- `5xx` responses except `501 Not Implemented`.

Retries use the generic `try` engine's backoff and jitter. A valid
`Retry-After` response header is honored and remains bounded by the configured
maximum delay.

Retries are enabled by default only for idempotent methods:

- `GET`
- `HEAD`
- `OPTIONS`
- `PUT`
- `DELETE`
- `TRACE`

`POST` and `PATCH` are not retried unless explicitly enabled:

```go
client := httptry.NewClient(
	httptry.WithUnsafeMethods(),
)
```

Only enable this when repeating the operation is safe, preferably with an
application-level idempotency key:

```go
req, err := httptry.NewRequest(ctx, http.MethodPost, url, bodyFactory)
if err != nil {
	return err
}
req.Header.Set("Idempotency-Key", requestID)
resp, err := client.Do(ctx, req)
```

## Request body replay

Use `NewRequest` when constructing requests that may be retried. It supports
ordinary readers, byte slices, strings, and body factories:

```go
req, err := httptry.NewRequest(ctx, http.MethodPost, url, func() (io.Reader, error) {
	return bytes.NewReader(payload), nil
})
```

The factory is called once for the initial attempt and again for each retry.
When a request has no `GetBody` function, `httptry` buffers its body before the
first attempt so it can replay it. Avoid this for very large or inherently
streaming bodies unless you provide an appropriate body factory.

`httptry` retries failures while sending the request or receiving response
headers. It returns the response body to the caller, so failures that occur
later while the caller reads `resp.Body` are not automatically retried.

## Configuration

```go
client := httptry.NewClient(
	httptry.WithRetryMax(5),
	httptry.WithRetryWaitMin(100*time.Millisecond),
	httptry.WithRetryWaitMax(10*time.Second),
	httptry.WithMaxElapsedTime(30*time.Second),
	httptry.WithRetryIf(func(resp *http.Response, err error) bool {
		if err != nil {
			return true
		}
		return resp != nil && resp.StatusCode == http.StatusTooManyRequests
	}),
)
```

`WithRetryMax` counts retries after the initial request. For example,
`WithRetryMax(3)` allows up to four total attempts.

If both `WithRetryIf` and compatibility-style `WithCheckRetry` are configured,
`CheckRetry` takes precedence for each attempt. Its boolean result replaces the
`RetryIf` decision; a non-nil error stops the operation.

## Retry statistics

Use `DoWithStats` when request timing and retry reasons matter:

```go
resp, stats, err := client.DoWithStats(ctx, req)
fmt.Println("attempts:", stats.Attempts)
fmt.Println("total duration:", stats.TotalDuration)
fmt.Println("backoff duration:", stats.BackoffDuration)
fmt.Println("final status:", stats.LastStatusCode)
```

Each `RetryReason` includes the failed attempt number, status code, underlying
error, and selected delay. `RetryStats.AttemptsDetail` contains the duration,
status code, and error for each HTTP attempt.

Use `WithOnAttempt` to observe attempts as they complete:

```go
client := httptry.NewClient(
	httptry.WithOnAttempt(func(attempt httptry.AttemptStats) {
		metrics.Observe("http_attempt_duration", attempt.Duration)
	}),
)
```

## Preparing retries

Use `WithPrepareRetry` to refresh credentials or modify a request after a
failed attempt:

```go
client := httptry.NewClient(
	httptry.WithPrepareRetry(func(req *http.Request, resp *http.Response, err error) error {
		if resp != nil && resp.StatusCode == http.StatusUnauthorized {
			req.Header.Set("Authorization", refreshedToken())
		}
		return nil
	}),
)
```

The hook receives the previous response and error before the next attempt.
For a retryable HTTP status, the error is a retryable status error; for a
transport or response-header failure, it is the underlying failure. The
previous response has already been drained and closed before this hook runs.

## Hooks and error handling

The package supports compatibility-style hooks:

```go
client := httptry.NewClient(
	httptry.WithRequestLogHook(func(logger httptry.Logger, req *http.Request, attempt int) {
		// attempt is zero-based.
	}),
	httptry.WithResponseLogHook(func(logger httptry.Logger, resp *http.Response) {
		// Observe each response.
	}),
	httptry.WithCheckRetry(func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		return true, nil
	}),
)
```

`WithErrorHandler` runs after retries are exhausted. A retryable response body
has already been drained and closed at that point; successful and non-retryable
responses remain owned by the caller.

Use `WithStatsErrorHandler` when the final handler needs complete attempt
statistics:

```go
httptry.WithStatsErrorHandler(func(
	resp *http.Response,
	err error,
	tries int,
	stats httptry.RetryStats,
) (*http.Response, error) {
	logFailure(stats.Attempts, stats.TotalDuration)
	return resp, err
})
```

## Standard `http.Client`

Use `StandardClient` when an existing API accepts `*http.Client`:

```go
retryingHTTP := httptry.NewClient(httptry.WithRetryMax(3)).StandardClient()
resp, err := retryingHTTP.Get(url)
```

The returned client uses the configured `httptry.Client` as its transport. Set
standard client fields such as `Timeout` on the returned `*http.Client` when
needed.

For direct transport composition, use the exported `RoundTripper`:

```go
standard := &http.Client{
	Timeout:   10 * time.Second,
	Transport: httptry.RoundTripper{Client: httptry.NewClient()},
}
```

## Migration from `go-retryablehttp`

| `go-retryablehttp` | `httptry` |
| --- | --- |
| `NewClient()` | `NewClient()` |
| `RetryMax` | `WithRetryMax` |
| `RetryWaitMin` | `WithRetryWaitMin` |
| `RetryWaitMax` | `WithRetryWaitMax` |
| `CheckRetry` | `WithCheckRetry` |
| `Backoff` | `WithBackoff` |
| `ErrorHandler` | `WithErrorHandler` |
| `PrepareRetry` | `WithPrepareRetry` |
| `RequestLogHook` | `WithRequestLogHook` |
| `ResponseLogHook` | `WithResponseLogHook` |
| `StandardClient()` | `StandardClient()` |

The main intentional behavior difference is safety: `POST` and `PATCH` are not
retried by default. Use `WithUnsafeMethods` only when repeating the operation
is safe.

## Integrations beyond HTTP

The underlying `try` engine can also be applied to gRPC, SQL connection
establishment, and AWS SDK middleware. See [INTEGRATIONS.md](INTEGRATIONS.md)
for dependency-light examples and guidance on avoiding unsafe retries.

## Testing

```bash
go test ./...
go test -race ./...
go vet ./...
```

The retry engine supports an injectable clock through `WithClock` for
deterministic backoff tests.

Context cancellation and deadline expiry are treated as normal control flow.
`httptry` does not emit built-in error logs for cancellation; applications can
classify these outcomes through `DoWithStats`, `WithOnAttempt`, or their own
request hooks.

## License

MIT