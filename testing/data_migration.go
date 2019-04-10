package testing

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/mock"
)

// MigrationFields will include the IDGenerator, TokenGenerator
// and whatever needed for testing.
type MigrationFields struct {
	IDGenerator    influxdb.IDGenerator
	TokenGenerator influxdb.TokenGenerator
	OldBuckets     []influxdb.OldBucket
	Organizations  []*influxdb.Organization
}

// MigrationNSearchService combines whever services required for testing.
type MigrationNSearchService interface {
	influxdb.BucketService
	influxdb.BucketMigrationService
}

// MigrationService tests all the service functions.
func MigrationService(
	init func(MigrationFields, *testing.T) (MigrationNSearchService, func()), t *testing.T,
) {
	tests := []struct {
		name string
		fn   func(init func(MigrationFields, *testing.T) (MigrationNSearchService, func()),
			t *testing.T)
	}{
		{
			name: "bucket migration",
			fn:   ConvertBucketToNew,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.fn(init, t)
		})
	}
}

// ConvertBucketToNew testing
func ConvertBucketToNew(
	init func(MigrationFields, *testing.T) (MigrationNSearchService, func()),
	t *testing.T,
) {
	type wants struct {
		err     error
		results []*influxdb.Bucket
	}
	tests := []struct {
		name   string
		fields MigrationFields
		wants  wants
	}{
		{
			name: "testing buckets conversion",
			fields: MigrationFields{
				IDGenerator: &loopIDGenerator{
					s: []string{oneID, twoID, threeID, fourID},
				},
				TokenGenerator: mock.NewTokenGenerator(oneToken, nil),
				Organizations: []*influxdb.Organization{
					{
						Name: "org1",
						ID:   MustIDBase16(oneID),
					},
					{
						Name: "org2",
						ID:   MustIDBase16(twoID),
					},
				},
				OldBuckets: []influxdb.OldBucket{
					{
						ID:                  MustIDBase16(twoID),
						OrganizationID:      MustIDBase16(oneID),
						Name:                "bucket2",
						Organization:        "org1",
						RetentionPolicyName: "rp1",
						RetentionPeriod:     100 * time.Minute,
					},
					{
						ID:                  MustIDBase16(threeID),
						OrganizationID:      MustIDBase16(twoID),
						Name:                "bucket3",
						Org:                 "org2",
						RetentionPolicyName: "rp2",
						RetentionPeriod:     101 * time.Minute,
					},
					{
						ID:                  MustIDBase16(fourID),
						OrganizationID:      MustIDBase16(twoID),
						Name:                "bucket4",
						Organization:        "org1",
						Org:                 "org2",
						RetentionPolicyName: "rp3",
						RetentionPeriod:     102 * time.Minute,
					},
				},
			},
			wants: wants{
				results: []*influxdb.Bucket{
					{
						ID:                  MustIDBase16(twoID),
						OrganizationID:      MustIDBase16(oneID),
						Name:                "bucket2",
						Org:                 "org1",
						RetentionPolicyName: "rp1",
						RetentionPeriod:     100 * time.Minute,
					},
					{
						ID:                  MustIDBase16(threeID),
						OrganizationID:      MustIDBase16(twoID),
						Name:                "bucket3",
						Org:                 "org2",
						RetentionPolicyName: "rp2",
						RetentionPeriod:     101 * time.Minute,
					},
					{
						ID:                  MustIDBase16(fourID),
						OrganizationID:      MustIDBase16(twoID),
						Name:                "bucket4",
						Org:                 "org2",
						RetentionPolicyName: "rp3",
						RetentionPeriod:     102 * time.Minute,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, done := init(tt.fields, t)
			defer done()
			ctx := context.Background()
			err := s.ConvertBucketToNew(ctx)
			ErrorsEqual(t, err, tt.wants.err)
			results, _, err := s.FindBuckets(ctx, influxdb.BucketFilter{})
			if err != nil {
				t.Fatalf("Err in FindBuckets: %v", err)
			}
			if diff := cmp.Diff(results, tt.wants.results); diff != "" {
				t.Errorf("converting results are different -got/+want\ndiff %s", diff)
			}
		})
	}

}
