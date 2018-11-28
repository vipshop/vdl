package metricsserver

type Mercury struct {
	Namespace  string `json:"namespace"`
	MetricName string `json:"metricName"`
	Endpoint   string `json:"endpoint"`

	Timestamp   int64   `json:"timestamp"`
	DoubleValue float64 `json:"doubleValue"`

	MercuryMeta
}

type MercuryMeta struct {
	Duration     int    `json:"duration"`
	CounterType  string `json:"counterType"`
	EndpointType string `json:"endpointType"`
}
