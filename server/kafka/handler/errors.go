// Copyright 2018 vip.com.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package handler

import "errors"

// Kafka Procotol Error Codes, See : https://kafka.apache.org/protocol

const (
	UNKNOWN                               = -1 //	The server experienced an unexpected error when processing the request
	NONE                                  = 0  //
	OFFSET_OUT_OF_RANGE                   = 1  //	The requested offset is not within the range of offsets maintained by the server.
	CORRUPT_MESSAGE                       = 2  //	This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.
	UNKNOWN_TOPIC_OR_PARTITION            = 3  //	This server does not host this topic-partition.
	INVALID_FETCH_SIZE                    = 4  //	The requested fetch size is invalid.
	LEADER_NOT_AVAILABLE                  = 5  //	There is no leader for this topic-partition as we are in the middle of a leadership election.
	NOT_LEADER_FOR_PARTITION              = 6  //	This server is not the leader for that topic-partition.
	REQUEST_TIMED_OUT                     = 7  //	The request timed out.
	BROKER_NOT_AVAILABLE                  = 8  //	The broker is not available.
	REPLICA_NOT_AVAILABLE                 = 9  //	The replica is not available for the requested topic-partition
	MESSAGE_TOO_LARGE                     = 10 //	The request included a message larger than the max message size the server will accept.
	STALE_CONTROLLER_EPOCH                = 11 //	The controller moved to another broker.
	OFFSET_METADATA_TOO_LARGE             = 12 //	The metadata field of the offset request was too large.
	NETWORK_EXCEPTION                     = 13 //	The server disconnected before a response was received.
	COORDINATOR_LOAD_IN_PROGRESS          = 14 //	The coordinator is loading and hence can't process requests.
	COORDINATOR_NOT_AVAILABLE             = 15 //	The coordinator is not available.
	NOT_COORDINATOR                       = 16 //	This is not the correct coordinator.
	INVALID_TOPIC_EXCEPTION               = 17 //	The request attempted to perform an operation on an invalid topic.
	RECORD_LIST_TOO_LARGE                 = 18 //	The request included message batch larger than the configured segment size on the server.
	NOT_ENOUGH_REPLICAS                   = 19 //	Messages are rejected since there are fewer in-sync replicas than required.
	NOT_ENOUGH_REPLICAS_AFTER_APPEND      = 20 //	Messages are written to the log, but to fewer in-sync replicas than required.
	INVALID_REQUIRED_ACKS                 = 21 //	Produce request specified an invalid value for required acks.
	ILLEGAL_GENERATION                    = 22 //	Specified group generation id is not valid.
	INCONSISTENT_GROUP_PROTOCOL           = 23 //	The group member's supported protocols are incompatible with those of existing members.
	INVALID_GROUP_ID                      = 24 //	The configured groupId is invalid
	UNKNOWN_MEMBER_ID                     = 25 //	The coordinator is not aware of this member.
	INVALID_SESSION_TIMEOUT               = 26 //	The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).
	REBALANCE_IN_PROGRESS                 = 27 //	The group is rebalancing, so a rejoin is needed.
	INVALID_COMMIT_OFFSET_SIZE            = 28 //	The committing offset data size is not valid
	TOPIC_AUTHORIZATION_FAILED            = 29 //	Not authorized to access topics: [Topic authorization failed.]
	GROUP_AUTHORIZATION_FAILED            = 30 //	Not authorized to access group: Group authorization failed.
	CLUSTER_AUTHORIZATION_FAILED          = 31 //	Cluster authorization failed.
	INVALID_TIMESTAMP                     = 32 //	The timestamp of the message is out of acceptable range.
	UNSUPPORTED_SASL_MECHANISM            = 33 //	The broker does not support the requested SASL mechanism.
	ILLEGAL_SASL_STATE                    = 34 //	Request is not valid given the current SASL state.
	UNSUPPORTED_VERSION                   = 35 //	The version of API is not supported.
	TOPIC_ALREADY_EXISTS                  = 36 //	Topic with this name already exists.
	INVALID_PARTITIONS                    = 37 //	Number of partitions is invalid.
	INVALID_REPLICATION_FACTOR            = 38 //	Replication-factor is invalid.
	INVALID_REPLICA_ASSIGNMENT            = 39 //	Replica assignment is invalid.
	INVALID_CONFIG                        = 40 //	Configuration is invalid.
	NOT_CONTROLLER                        = 41 //	This is not the correct controller for this cluster.
	INVALID_REQUEST                       = 42 //	This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.
	UNSUPPORTED_FOR_MESSAGE_FORMAT        = 43 //	The message format version on the broker does not support the request.
	POLICY_VIOLATION                      = 44 //	Request parameters do not satisfy the configured policy.
	OUT_OF_ORDER_SEQUENCE_NUMBER          = 45 //	The broker received an out of order sequence number
	DUPLICATE_SEQUENCE_NUMBER             = 46 //	The broker received a duplicate sequence number
	INVALID_PRODUCER_EPOCH                = 47 //	Producer attempted an operation with an old epoch. Either there is a newer producer with the same transactionalId, or the producer's transaction has been expired by the broker.
	INVALID_TXN_STATE                     = 48 //	The producer attempted a transactional operation in an invalid state
	INVALID_PRODUCER_ID_MAPPING           = 49 //	The producer attempted to use a producer id which is not currently assigned to its transactional id
	INVALID_TRANSACTION_TIMEOUT           = 50 //	The transaction timeout is larger than the maximum value allowed by the broker (as configured by max.transaction.timeout.ms).
	CONCURRENT_TRANSACTIONS               = 51 //	The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing
	TRANSACTION_COORDINATOR_FENCED        = 52 //	Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer
	TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53 //	Transactional Id authorization failed
	SECURITY_DISABLED                     = 54 //	Security features are disabled.
	OPERATION_NOT_ATTEMPTED               = 55 //	The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.
)

var (
	ErrCrcNotMatch        = errors.New("protocol: Crc32 values do not match")
	ErrNotSupport         = errors.New("protocol:not support this feature")
	ErrBrokerNotAvailable = errors.New("protocol:the broker is not available")
)
