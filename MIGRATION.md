# Migrating from go-retryablehttp

`httptry` is designed to make the common migration path small while providing
the generic `try` engine underneath. The most important intentional difference
is that unsafe methods are not retried by default.

## Requirements

`httptry` requires Go 1.27 or newer. The current module also depends on the
generic `github.com/nodivbyzero/try` module at Go 1.27. Check the toolchain
used by CI and production builds before starting the migration.

## Default values

The defaults are not identical, so a pure constructor rename can change retry
cadence:

| Setting | go-retryablehttp | httptry |
| --- | --- | --- |
| Maximum retries | 4 retries after the initial attempt | 4 retries after the initial attempt (`WithAttempts(5)`) |
| Minimum wait | 1 second | 200 milliseconds |
| Maximum wait | 30 seconds | 30 seconds |
| Default logger | Standard logger | No built-in logger |

Set `WithRetryWaitMin`, `WithRetryWaitMax`, and `WithRetryMax` explicitly when
preserving an existing production retry cadence matters.

## Basic GET

Before:

```go
client := retryablehttp.NewClient()
client.RetryMax = 3

req, err := retryablehttp.NewRequest("GET", url, nil)
if err != nil {
	return err
}
resp, err := client.Do(req)
```

After:

```go
client := httptry.NewClient(
	httptry.WithRetryMax(3),
)

req, err := httptry.NewRequest(ctx, http.MethodGet, url, nil)
if err != nil {
	return err
}
resp, err := client.Do(ctx, req)
```

For simple requests, the convenience API is shorter:

```go
resp, err := httptry.NewClient(
	httptry.WithRetryMax(3),
).Get(url)
```

## Standard `http.Client` integration

Before:

```go
retryClient := retryablehttp.NewClient()
retryClient.RetryMax = 5
standardClient := retryClient.StandardClient()
```

After:

```go
standardClient := httptry.NewClient(
	httptry.WithRetryMax(5),
).StandardClient()
```

You can also compose the exported transport directly:

```go
standardClient := &http.Client{
	Timeout:   10 * time.Second,
	Transport: httptry.RoundTripper{Client: httptry.NewClient()},
}
```

## Replayable request bodies

Before:

```go
req, err := retryablehttp.NewRequest("POST", url, payload)
if err != nil {
	return err
}
resp, err := client.Do(req)
```

After:

```go
req, err := httptry.NewRequest(ctx, http.MethodPost, url, func() (io.Reader, error) {
	return bytes.NewReader(payload), nil
})
if err != nil {
	return err
}
req.Header.Set("Idempotency-Key", requestID)

client := httptry.NewClient(httptry.WithUnsafeMethods())
resp, err := client.Do(ctx, req)
```

`POST` and `PATCH` are deliberately not retried unless
`WithUnsafeMethods()` is configured. Use an idempotency key or an equivalent
application-level guarantee before enabling them.

## Retry configuration mapping

| go-retryablehttp | httptry |
| --- | --- |
| `RetryMax` | `WithRetryMax` |
| `RetryWaitMin` | `WithRetryWaitMin` |
| `RetryWaitMax` | `WithRetryWaitMax` |
| `CheckRetry` | `WithCheckRetry` |
| `Backoff` | `WithBackoff` |
| `ErrorHandler` | `WithErrorHandler` |
| `PrepareRetry` | `WithPrepareRetry` |
| `RequestLogHook` | `WithRequestLogHook` |
| `ResponseLogHook` | `WithResponseLogHook` |
| `HTTPClient` | `WithHTTPClient` |
| `StandardClient()` | `StandardClient()` |

`WithStatsErrorHandler` is an `httptry` addition that supplies complete
`RetryStats` to the final error handler.

When both `WithRetryIf` and `WithCheckRetry` are configured, the compatibility
callback wins for each attempt. Its boolean result replaces the `RetryIf`
decision, and a non-nil error stops the operation. This makes it possible to
bring an existing `CheckRetry` policy across without accidentally combining it
with the default policy.

`WithPrepareRetry` receives both the previous response and the reason for the
retry. For a retryable status response, the error is a retryable status error;
for transport or response-header failures, it is the underlying error. The
previous response has already been drained and closed before the hook runs.

Final errors preserve the underlying transport error through Go's standard
`Unwrap` chain. Existing `errors.Is` and `errors.As` checks for network or
other transport errors can continue to work after migration.

## Behavior differences

| Behavior | go-retryablehttp | httptry |
| --- | --- | --- |
| Default retry methods | Broad HTTP retry policy | Safe/idempotent methods only |
| `POST`/`PATCH` retries | Possible by default policy | Opt-in with `WithUnsafeMethods` |
| Retry timing | Retry count and backoff | Count, context deadline, and independent elapsed-time limit |
| `Retry-After` | HTTP-specific backoff handling | Seconds and HTTP dates for retryable responses, bounded by configured maximum |
| Retry metadata | Hooks provide limited data | Built-in `RetryStats`, `AttemptStats`, and `OnAttempt` |
| Retry preparation | No native credential-refresh hook | `WithPrepareRetry` receives previous response and error |
| Retry policy precedence | `CheckRetry` policy | `WithCheckRetry` takes precedence over `WithRetryIf` |
| Error inspection | Underlying error depends on handler | `errors.Is`/`errors.As` can unwrap transport errors |
| Generic retry engine | HTTP-specific | Shared with non-HTTP operations through `try` |
| Response-body read errors | Caller-managed | Caller-managed; not retried after `Do` returns |

## Recommended migration sequence

1. Start with `StandardClient()` or `NewClient` plus `WithRetryMax`.
2. Run existing integration tests against representative failures.
3. Review all `POST` and `PATCH` call sites.
4. Add replayable body factories and idempotency keys where retries are safe.
5. Replace custom logging with `WithOnAttempt` or `DoWithStats` where retry
   timing and attempt reasons are operationally important.
6. Compare the final behavior with the compatibility matrix above before
   removing the old dependency.
