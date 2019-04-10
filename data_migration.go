package influxdb

import "time"

// DataMigrationService will transfer the old stored data to new released version.
type DataMigrationService interface {
	// IsMigrated will determine if data already migrated.
	// Usually we will put an index to make sure this the newest version.
	IsMigrated() bool
	// ConvertToNew to do a scan to the storage and convert every thing related.
	ConvertToNew() error
	BucketMigrationService
}

// BucketIsMigratedIndex will be the index check to determine
// if the newest bucket schema is applied.
var BucketIsMigratedIndex = []byte("bucketIsMigrated_org")

// BucketMigrationService will migrate old bucket to the most recent bucket schema.
type BucketMigrationService interface {
	IsBucketMigrated() bool
	ConvertBucketToNew() error
}

// OldBucket should includes all old fields of previous bucket schemas,
// as well as the new fields incase of any partial conversion.
type OldBucket struct {
	ID                  ID            `json:"id,omitempty"`
	OrganizationID      ID            `json:"orgID,omitempty"`
	Organization        string        `json:"organization,omitempty"`
	Org                 string        `json:"org,omitempty"`
	Name                string        `json:"name"`
	RetentionPolicyName string        `json:"rp,omitempty"` // This to support v1 sources
	RetentionPeriod     time.Duration `json:"retentionPeriod"`
}

// ConvertOldBucketToNew convert to old bucket to new.
func ConvertOldBucketToNew(old OldBucket) Bucket {
	org := old.Org
	if org == "" && old.Organization != "" {
		org = old.Organization
	}
	return Bucket{
		ID:                  old.ID,
		OrganizationID:      old.OrganizationID,
		Org:                 org,
		Name:                old.Name,
		RetentionPolicyName: old.RetentionPolicyName,
		RetentionPeriod:     old.RetentionPeriod,
	}
}
