package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Probe performs the minimal HTTP checks expected of a running
// scheduler: the health endpoint must answer and the console must
// render.
func Probe(base string) error {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(strings.TrimRight(base, "/") + "/healthz")
	if err != nil {
		return err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 512))
	resp.Body.Close()
	if readErr != nil {
		return readErr
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("healthz returned %s", resp.Status)
	}
	if len(body) == 0 {
		return fmt.Errorf("healthz returned an empty body")
	}
	resp2, err := client.Get(strings.TrimRight(base, "/") + "/")
	if err != nil {
		return err
	}
	page, readErr := io.ReadAll(io.LimitReader(resp2.Body, 8192))
	resp2.Body.Close()
	if readErr != nil {
		return readErr
	}
	if resp2.StatusCode != http.StatusOK {
		return fmt.Errorf("console returned %s", resp2.Status)
	}
	if !strings.Contains(string(page), "JobScheduler Console") {
		return fmt.Errorf("console page marker is missing")
	}
	return nil
}

// probeWithRetry retries the startup probe while the listener warms up.
func probeWithRetry(base string, attempts int) error {
	var last error
	for i := 0; i < attempts; i++ {
		if err := Probe(base); err == nil {
			return nil
		} else {
			last = err
		}
		time.Sleep(200 * time.Millisecond)
	}
	return last
}
