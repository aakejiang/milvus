package metastore

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	// TimestampPrefix prefix for timestamp
	TimestampPrefix = ComponentPrefix + "/timestamp"

	// DDOperationPrefix prefix for DD operation
	DDOperationPrefix = ComponentPrefix + "/dd-operation"

	// DDMsgSendPrefix prefix to indicate whether DD msg has been send
	DDMsgSendPrefix = ComponentPrefix + "/dd-msg-send"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	// SegmentIndexMetaPrefix prefix for segment index meta
	SegmentIndexMetaPrefix = ComponentPrefix + "/segment-index"

	// IndexMetaPrefix prefix for index meta
	IndexMetaPrefix = ComponentPrefix + "/index"

	// CollectionAliasMetaPrefix prefix for collection alias meta
	CollectionAliasMetaPrefix = ComponentPrefix + "/collection-alias"

	// UserSubPrefix subpath for credential user
	UserSubPrefix = "/credential/users"

	// CredentialPrefix prefix for credential user
	CredentialPrefix = ComponentPrefix + UserSubPrefix
)

// DdOperation used to save ddMsg into etcd
type DdOperation struct {
	Body []byte `json:"body"`
	Type string `json:"type"`
}
