package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

var serverURL string

func main() {
	root := &cobra.Command{
		Use:   "tq",
		Short: "tq — task queue CLI",
	}
	root.PersistentFlags().StringVar(&serverURL, "server", envOr("TQ_SERVER", "http://localhost:8080"), "server URL")

	root.AddCommand(
		submitCmd(),
		statusCmd(),
		listCmd(),
		statsCmd(),
		dlqCmd(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

// tq submit --queue=emails --payload='{"to":"a@b.com"}' [--priority=5] [--retries=3] [--run-at=<RFC3339>]
func submitCmd() *cobra.Command {
	var queueName, payload, runAt string
	var priority, maxRetries int

	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a new job to the queue",
		RunE: func(cmd *cobra.Command, _ []string) error {
			var rawPayload interface{}
			if err := json.Unmarshal([]byte(payload), &rawPayload); err != nil {
				return fmt.Errorf("payload must be valid JSON: %w", err)
			}

			body := map[string]interface{}{
				"queue_name":  queueName,
				"payload":     rawPayload,
				"priority":    priority,
				"max_retries": maxRetries,
			}
			if runAt != "" {
				body["run_at"] = runAt
			}

			resp, err := postJSON(serverURL+"/jobs", body)
			if err != nil {
				return err
			}
			printJSON(resp)
			return nil
		},
	}
	cmd.Flags().StringVarP(&queueName, "queue", "q", "default", "Queue name")
	cmd.Flags().StringVarP(&payload, "payload", "p", "{}", "JSON payload")
	cmd.Flags().IntVar(&priority, "priority", 0, "Job priority (higher = processed first)")
	cmd.Flags().IntVar(&maxRetries, "retries", 3, "Max retry attempts before DLQ")
	cmd.Flags().StringVar(&runAt, "run-at", "", "Schedule job for a future time (RFC3339)")
	return cmd
}

// tq status <job-id>
func statusCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "status <job-id>",
		Short: "Get the status of a specific job",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			resp, err := getJSON(serverURL + "/jobs/" + args[0])
			if err != nil {
				return err
			}
			printJobDetail(resp)
			return nil
		},
	}
}

// tq list [--queue=default] [--status=pending] [--limit=20]
func listCmd() *cobra.Command {
	var queueName, status string
	var limit, offset int

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List jobs",
		RunE: func(_ *cobra.Command, _ []string) error {
			u := fmt.Sprintf("%s/jobs?queue=%s&status=%s&limit=%d&offset=%d",
				serverURL, queueName, status, limit, offset)
			resp, err := getJSON(u)
			if err != nil {
				return err
			}
			printJobList(resp)
			return nil
		},
	}
	cmd.Flags().StringVarP(&queueName, "queue", "q", "", "Filter by queue name")
	cmd.Flags().StringVarP(&status, "status", "s", "", "Filter by status (pending|running|completed|failed|dead)")
	cmd.Flags().IntVar(&limit, "limit", 20, "Max results")
	cmd.Flags().IntVar(&offset, "offset", 0, "Offset for pagination")
	return cmd
}

// tq stats --queue=default
func statsCmd() *cobra.Command {
	var queueName string

	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Show queue statistics",
		RunE: func(_ *cobra.Command, _ []string) error {
			if queueName == "" {
				return fmt.Errorf("--queue is required")
			}
			resp, err := getJSON(serverURL + "/queues/" + queueName + "/stats")
			if err != nil {
				return err
			}
			printStats(resp)
			return nil
		},
	}
	cmd.Flags().StringVarP(&queueName, "queue", "q", "", "Queue name")
	return cmd
}

// tq dlq list [--queue=] | tq dlq requeue <dlq-id>
func dlqCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dlq",
		Short: "Dead letter queue operations",
	}

	var queueName string
	var limit int

	listDLQ := &cobra.Command{
		Use:   "list",
		Short: "List dead letter jobs",
		RunE: func(_ *cobra.Command, _ []string) error {
			u := fmt.Sprintf("%s/dlq?queue=%s&limit=%d", serverURL, queueName, limit)
			resp, err := getJSON(u)
			if err != nil {
				return err
			}
			printDLQ(resp)
			return nil
		},
	}
	listDLQ.Flags().StringVarP(&queueName, "queue", "q", "", "Filter by queue name")
	listDLQ.Flags().IntVar(&limit, "limit", 20, "Max results")

	requeueDLQ := &cobra.Command{
		Use:   "requeue <dlq-id>",
		Short: "Requeue a dead letter job back to pending",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			resp, err := postJSON(serverURL+"/dlq/"+args[0]+"/requeue", nil)
			if err != nil {
				return err
			}
			fmt.Println("Requeued as new job:")
			printJSON(resp)
			return nil
		},
	}

	cmd.AddCommand(listDLQ, requeueDLQ)
	return cmd
}

// ---- HTTP helpers ----

func getJSON(url string) (map[string]interface{}, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	return decodeBody(resp)
}

func postJSON(url string, body interface{}) (map[string]interface{}, error) {
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}
	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	return decodeBody(resp)
}

func decodeBody(resp *http.Response) (map[string]interface{}, error) {
	data, _ := io.ReadAll(resp.Body)
	var out map[string]interface{}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("decode response: %w (body: %s)", err, data)
	}
	if errMsg, ok := out["error"]; ok {
		return nil, fmt.Errorf("server error: %v", errMsg)
	}
	return out, nil
}

// ---- Pretty printers ----

func printJSON(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func printJobDetail(m map[string]interface{}) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fields := []string{"id", "queue_name", "status", "priority", "retry_count", "max_retries",
		"next_run_at", "started_at", "completed_at", "error_message", "created_at"}
	for _, f := range fields {
		if v, ok := m[f]; ok && v != nil {
			fmt.Fprintf(w, "%s\t%v\n", f, v)
		}
	}
	w.Flush()
}

func printJobList(m map[string]interface{}) {
	jobs, _ := m["jobs"].([]interface{})
	if len(jobs) == 0 {
		fmt.Println("no jobs found")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tQUEUE\tSTATUS\tRETRIES\tCREATED")
	for _, raw := range jobs {
		j, _ := raw.(map[string]interface{})
		fmt.Fprintf(w, "%v\t%v\t%v\t%v/%v\t%v\n",
			j["id"], j["queue_name"], j["status"],
			j["retry_count"], j["max_retries"],
			formatTime(j["created_at"]),
		)
	}
	w.Flush()
}

func printStats(m map[string]interface{}) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Queue:\t%v\n", m["queue_name"])
	fmt.Fprintf(w, "Pending:\t%v\n", m["pending"])
	fmt.Fprintf(w, "Running:\t%v\n", m["running"])
	fmt.Fprintf(w, "Completed:\t%v\n", m["completed"])
	fmt.Fprintf(w, "Failed:\t%v\n", m["failed"])
	fmt.Fprintf(w, "Dead (DLQ):\t%v\n", m["dead"])
	fmt.Fprintf(w, "Total:\t%v\n", m["total"])
	w.Flush()
}

func printDLQ(m map[string]interface{}) {
	jobs, _ := m["dead_letter_jobs"].([]interface{})
	if len(jobs) == 0 {
		fmt.Println("dead letter queue is empty")
		return
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DLQ ID\tORIGINAL JOB ID\tQUEUE\tRETRIES\tERROR\tCREATED")
	for _, raw := range jobs {
		j, _ := raw.(map[string]interface{})
		errMsg := fmt.Sprintf("%v", j["error_message"])
		if len(errMsg) > 40 {
			errMsg = errMsg[:40] + "…"
		}
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%s\t%v\n",
			j["id"], j["original_job_id"], j["queue_name"],
			j["retry_count"], errMsg, formatTime(j["created_at"]),
		)
	}
	w.Flush()
}

func formatTime(v interface{}) string {
	s, _ := v.(string)
	if s == "" {
		return "-"
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
