package conf

const (
	defaultMasterTimeout = 30   // 30 ms
	defaultOpTimeout     = 6000 // 6 s
)

var (
	MultiConfig *MultiHBaseConf
)

type HBaseConf struct {
	ZkRoot string
	Name   string
}

type MultiHBaseConf struct {
	QueryMasterTimeout  int
	MutateMasterTimeout int
	ScanMasterTimeout   int

	HBaseConfigs map[string]*HBaseConf
	QueryMaster  string
	MutateMaster string
	ScanMaster   string

	OperationTimeout int
}

type KafkaConf struct {
	Brokers    []string
	WriteTopic string
	Switch     bool
	QueueSize  int
}
