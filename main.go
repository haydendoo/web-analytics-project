package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
    "github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/joho/godotenv"
)

var (
	kc         *kinesis.Client
	smClient    *sagemakerruntime.Client
	streamName string
	endpointName string
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("no .env file, using environment variables")
	}
	streamName = os.Getenv("KINESIS_STREAM_NAME")

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	kc = kinesis.NewFromConfig(cfg)

	smClient = sagemakerruntime.NewFromConfig(cfg)
	endpointName = os.Getenv("SAGEMAKER_ENDPOINT_NAME")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "index.html")
	})
	http.HandleFunc("/track", track)
	http.HandleFunc("/check-conversion", checkConversion)

	log.Println("Listening on :80")
	log.Fatal(http.ListenAndServe("0.0.0.0:80", nil))
}

func checkConversion(w http.ResponseWriter, r *http.Request) {
	var features map[string]any

	if err := json.NewDecoder(r.Body).Decode(&features); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// 1. IMPORTANT: This list MUST exactly match the columns in your 
	// training DataFrame (X) and be in the EXACT same order.
	// Based on your Python code, it's everything in 'df' EXCEPT 'session_id' and 'converted'
	featureKeys := []string{
		"total_events",
		"total_button_clicks",
		"session_duration_sec",
		"avg_distance_to_checkout",
		"unique_elements_clicked",
		"button_click_ratio",
	}

	var values []string
	for _, key := range featureKeys {
		val, exists := features[key]
		
		if !exists || val == nil {
			// If a value is missing, send "0" or "0.0" 
			// XGBoost cannot handle the string "<nil>"
			values = append(values, "0")
		} else {
			// Convert the value to a string. 
			// If it's a number from JSON, %v works fine.
			values = append(values, fmt.Sprintf("%v", val))
		}
	}

	// 2. Join into CSV (e.g., "1.2,0,44.5")
	csvPayload := strings.Join(values, ",")

	// 3. Invoke Endpoint
	resp, err := smClient.InvokeEndpoint(r.Context(), &sagemakerruntime.InvokeEndpointInput{
		EndpointName: aws.String(endpointName),
		ContentType:  aws.String("text/csv"),
		Body:         []byte(csvPayload),
	})

	if err != nil {
		log.Printf("SageMaker error: %v", err)
		// This will show you exactly what CSV was sent if it fails again
		log.Printf("Payload sent: %s", csvPayload) 
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	predictionStr := strings.TrimSpace(string(resp.Body))

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"prediction": predictionStr,
	})
}

func track(w http.ResponseWriter, r *http.Request) {
	var event map[string]any
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	data, _ := json.Marshal(event)
	partitionKey, _ := event["session_id"].(string)
	if partitionKey == "" {
		partitionKey = "default"
	}

	_, err := kc.PutRecord(r.Context(), &kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		Data:         data,
		PartitionKey: aws.String(partitionKey),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"ok":true}`))
}
