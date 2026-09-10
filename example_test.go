package httptry_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/nodivbyzero/httptry"
)

func ExampleNewClient() {
	client := httptry.NewClient(
		httptry.WithRetryMax(3),
		httptry.WithRetryWaitMin(100*time.Millisecond),
		httptry.WithRetryWaitMax(5*time.Second),
	)

	resp, err := client.Get("https://api.example.com/health")
	if err != nil {
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
}

func ExampleClient_Do() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := httptry.NewRequest(ctx, http.MethodGet, "https://api.example.com/users", nil)
	if err != nil {
		return
	}
	resp, err := httptry.NewClient().Do(ctx, req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
}

func ExampleNewRequest_bodyFactory() {
	ctx := context.Background()
	payload := []byte(`{"name":"Ada"}`)

	req, err := httptry.NewRequest(ctx, http.MethodPost, "https://api.example.com/users", func() (io.Reader, error) {
		return bytes.NewReader(payload), nil
	})
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	client := httptry.NewClient(httptry.WithUnsafeMethods())
	resp, err := client.Do(ctx, req)
	if resp != nil {
		defer resp.Body.Close()
	}
	_ = err
}

func ExampleClient_DoWithStats() {
	ctx := context.Background()
	req, err := httptry.NewRequest(ctx, http.MethodGet, "https://api.example.com/health", nil)
	if err != nil {
		return
	}

	client := httptry.NewClient(httptry.WithRetryMax(2))
	resp, stats, err := client.DoWithStats(ctx, req)
	if resp != nil {
		defer resp.Body.Close()
	}
	if err == nil {
		fmt.Println("attempts:", stats.Attempts)
		fmt.Println("duration:", stats.TotalDuration)
	}
}

func ExampleWithPrepareRetry() {
	client := httptry.NewClient(
		httptry.WithPrepareRetry(func(req *http.Request, resp *http.Response, err error) error {
			if resp != nil && resp.StatusCode == http.StatusUnauthorized {
				req.Header.Set("Authorization", "Bearer refreshed-token")
			}
			return nil
		}),
	)

	resp, err := client.Get("https://api.example.com/protected")
	if resp != nil {
		defer resp.Body.Close()
	}
	_ = err
}

func ExampleClient_compatibilityHooks() {
	client := httptry.NewClient(
		httptry.WithCheckRetry(func(ctx context.Context, resp *http.Response, err error) (bool, error) {
			if err != nil {
				return true, nil
			}
			return resp != nil && resp.StatusCode >= 500, nil
		}),
		httptry.WithBackoff(func(min, max time.Duration, attempt int, resp *http.Response) time.Duration {
			return min * time.Duration(attempt)
		}),
		httptry.WithRequestLogHook(func(logger httptry.Logger, req *http.Request, attempt int) {}),
		httptry.WithResponseLogHook(func(logger httptry.Logger, resp *http.Response) {}),
		httptry.WithErrorHandler(func(resp *http.Response, err error, tries int) (*http.Response, error) {
			return resp, err
		}),
	)

	resp, err := client.Get("https://api.example.com/health")
	if resp != nil {
		defer resp.Body.Close()
	}
	_ = err
}

func ExampleWithStatsErrorHandler() {
	client := httptry.NewClient(
		httptry.WithStatsErrorHandler(func(resp *http.Response, err error, tries int, stats httptry.RetryStats) (*http.Response, error) {
			fmt.Println("attempts:", stats.Attempts)
			return resp, err
		}),
	)

	resp, err := client.Get("https://api.example.com/health")
	if resp != nil {
		defer resp.Body.Close()
	}
	_ = err
}

func ExampleClient_StandardClient() {
	retryingClient := httptry.NewClient(httptry.WithRetryMax(3)).StandardClient()
	retryingClient.Timeout = 10 * time.Second

	resp, err := retryingClient.Get("https://api.example.com/health")
	if resp != nil {
		defer resp.Body.Close()
	}
	_ = err
}

func ExampleRoundTripper() {
	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: httptry.RoundTripper{Client: httptry.NewClient()},
	}

	resp, err := client.Get("https://api.example.com/health")
	if resp != nil {
		defer resp.Body.Close()
	}
	_ = err
}
