# Integration examples

`httptry` is the HTTP adapter. The same `try.Do` engine can also be used at
other client boundaries without adding gRPC, SQL, or AWS SDK dependencies to
this module. The examples below use the current APIs from those ecosystems;
pin their versions in the application that adopts them.

## gRPC unary client interceptor

Retry only calls that are safe to repeat. In particular, `codes.Unavailable`
and `codes.ResourceExhausted` are commonly transient, while an arbitrary
application RPC should not be retried merely because it returned an error.
The interceptor below retries unary calls with a bounded attempt count and
honors cancellation through the context passed to `try.Do`.

```go
package grpcretry

import (
	"context"
	"time"

	"github.com/nodivbyzero/try"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any,
		cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		_, err := try.Do(ctx, func(attemptCtx context.Context) (struct{}, error) {
			return struct{}{}, invoker(attemptCtx, method, req, reply, cc, opts...)
		},
			try.WithAttempts(3),
			try.WithInitialDelay(50*time.Millisecond),
			try.WithMaxDelay(time.Second),
			try.WithRetryIf(func(err error) bool {
				code := status.Code(err)
				return code == codes.Unavailable || code == codes.ResourceExhausted
			}),
		)
		return err
	}
}

// conn, err := grpc.NewClient(target,
//     grpc.WithTransportCredentials(creds),
//     grpc.WithUnaryInterceptor(UnaryClientInterceptor()),
// )
```

For streaming RPCs, use a stream interceptor with an explicit policy. Do not
blindly recreate a stream after any error: messages may already have produced
side effects, and replay requires application-level sequence or idempotency
semantics.

## SQL connector wrapper

A SQL wrapper should retry connection establishment and explicitly selected
read-only operations. Retrying `ExecContext`, transactions, or writes by
default can duplicate side effects. The small wrapper below retries connector
creation, which is generally the safest useful SQL integration; `database/sql`
will continue to manage pooling and connection lifetime.

```go
package sqlretry

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/nodivbyzero/try"
)

type Connector struct {
	Base    driver.Connector
	Options []try.Option
}

func (c Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := try.Do(ctx, func(ctx context.Context) (driver.Conn, error) {
		return c.Base.Connect(ctx)
	}, append([]try.Option{
		try.WithAttempts(3),
		try.WithInitialDelay(25 * time.Millisecond),
		try.WithMaxDelay(time.Second),
		try.WithRetryIf(func(err error) bool {
			// Replace this with driver/vendor-specific transient-error
			// classification. Never return true for a known permanent error.
			return err != nil
		}),
	}, c.Options...)...)
	return conn, err
}

func (c Connector) Driver() driver.Driver { return c.Base.Driver() }
```

Use it with `sql.OpenDB(sqlretry.Connector{Base: connector})`. For query
retries, put the retry around a complete read-only function that creates a
fresh query and consumes or closes its rows on every attempt. Do not retry a
partially consumed `Rows`, an active transaction, or a write unless the
database operation has an application-level idempotency guarantee.

## AWS SDK for Go v2 middleware

AWS SDK v2 already has a retryer. Prefer configuring that retryer first. Use a
custom Smithy middleware when an application needs `try` policy, metrics, or a
shared retry budget around a complete SDK operation. The middleware below
wraps the operation's `next.Handle` call and retries only selected throttling
and availability errors.

```go
package awsretry

import (
	"context"
	"time"

	"github.com/nodivbyzero/try"
	"github.com/aws/smithy-go/middleware"
	"github.com/aws/smithy-go/transport/http"
	"github.com/aws/smithy-go"
)

type RetryMiddleware struct{}

func (RetryMiddleware) ID() string { return "httptry-aws-retry" }

func (RetryMiddleware) HandleSerialize(ctx context.Context, in middleware.SerializeInput,
		next middleware.SerializeHandler) (out middleware.SerializeOutput, metadata middleware.Metadata, err error) {
	return try.Do(ctx, func(ctx context.Context) (middleware.SerializeOutput, error) {
		return next.HandleSerialize(ctx, in)
	},
		try.WithAttempts(3),
		try.WithInitialDelay(100*time.Millisecond),
		try.WithMaxDelay(2*time.Second),
		try.WithRetryIf(func(err error) bool {
			var responseErr *http.ResponseError
			if !smithy.As(err, &responseErr) {
				return false
			}
			return responseErr.HTTPStatusCode() == 429 || responseErr.HTTPStatusCode() >= 500
		}),
	)
}
```

The AWS SDK's retry stack normally operates around the complete request and
has service-aware classification. If custom middleware is used, install it at
the appropriate stack step for the SDK operation, avoid layering two
independent retry loops without a total budget, and preserve request signing
and body replay behavior. For mutating operations, use the service's
idempotency token support before enabling retries.

## Shared guidance

- Pass the operation context to `try.Do`; cancellation stops both attempts and
  backoff.
- Classify errors narrowly and keep the attempt count bounded.
- Retry only idempotent or explicitly deduplicated operations.
- Record attempt details with `try.WithOnAttempt` when latency and retry
  reasons need to be observed.
- Use a test clock (`try.WithClock`) to make backoff tests deterministic.
