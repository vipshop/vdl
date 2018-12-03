package protocol

// Protocol API keys. See: https://kafka.apache.org/protocol#protocol_api_keys
const (
	ProduceKey              = 0
	FetchKey                = 1
	OffsetsKey              = 2
	MetadataKey             = 3
	LeaderAndISRKey         = 4
	StopReplicaKey          = 5
	UpdateMetadataKey       = 6
	ControlledShutdownKey   = 7
	OffsetCommitKey         = 8
	OffsetFetchKey          = 9
	FindCoordinator         = 10 //TODO. ??
	JoinGroupKey            = 11
	HeartbeatKey            = 12
	LeaveGroupKey           = 13
	SyncGroupKey            = 14
	DescribeGroupsKey       = 15
	ListGroupsKey           = 16
	SaslHandshakeKey        = 17
	APIVersionsKey          = 18
	CreateTopicsKey         = 19
	DeleteTopicsKey         = 20
	DeleteRecordsKey        = 21
	InitProducerIdKey       = 22
	OffsetForLeaderEpochKey = 23
	AddPartitionsToTxnKey   = 24
	AddOffsetsToTxnKey      = 25
	EndTxnKey               = 26
	WriteTxnMarkersKey      = 27
	TxnOffsetCommitKey      = 28
	DescribeAclsKey         = 29
	CreateAclsKey           = 30
	DeleteAclsKey           = 31
	DescribeConfigsKey      = 32
	AlterConfigsKey         = 33
)

var (
	KeyToString = map[int16]string{
		ProduceKey:              "ProduceKey",
		FetchKey:                "FetchKey",
		OffsetsKey:              "OffsetsKey",
		MetadataKey:             "MetadataKey",
		LeaderAndISRKey:         "LeaderAndISRKey",
		StopReplicaKey:          "StopReplicaKey",
		UpdateMetadataKey:       "UpdateMetadataKey",
		ControlledShutdownKey:   "ControlledShutdownKey",
		OffsetCommitKey:         "ControlledShutdownKey",
		OffsetFetchKey:          "OffsetFetchKey",
		FindCoordinator:         "FindCoordinator",
		JoinGroupKey:            "JoinGroupKey",
		HeartbeatKey:            "HeartbeatKey",
		LeaveGroupKey:           "LeaveGroupKey",
		SyncGroupKey:            "SyncGroupKey",
		DescribeGroupsKey:       "DescribeGroupsKey",
		ListGroupsKey:           "ListGroupsKey",
		SaslHandshakeKey:        "SaslHandshakeKey",
		APIVersionsKey:          "APIVersionsKey",
		CreateTopicsKey:         "CreateTopicsKey",
		DeleteTopicsKey:         "DeleteTopicsKey",
		DeleteRecordsKey:        "DeleteRecordsKey",
		InitProducerIdKey:       "InitProducerIdKey",
		OffsetForLeaderEpochKey: "OffsetForLeaderEpochKey",
		AddPartitionsToTxnKey:   "AddPartitionsToTxnKey",
		AddOffsetsToTxnKey:      "AddOffsetsToTxnKey",
		EndTxnKey:               "EndTxnKey",
		WriteTxnMarkersKey:      "WriteTxnMarkersKey",
		TxnOffsetCommitKey:      "TxnOffsetCommitKey",
		DescribeAclsKey:         "DescribeAclsKey",
		CreateAclsKey:           "CreateAclsKey",
		DeleteAclsKey:           "DeleteAclsKey",
		DescribeConfigsKey:      "DescribeConfigsKey",
		AlterConfigsKey:         "AlterConfigsKey",
	}
)
