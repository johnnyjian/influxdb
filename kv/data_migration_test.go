package kv_test

import (
	"context"
	"testing"

	"github.com/influxdata/influxdb/kv"
	influxdbtesting "github.com/influxdata/influxdb/testing"
)

func TestBoltMigrationService(t *testing.T) {
	influxdbtesting.MigrationService(initBoltMigrationService, t)
}

func TestInmemMigrationService(t *testing.T) {
	influxdbtesting.MigrationService(initInmemMigrationService, t)
}

func initBoltMigrationService(f influxdbtesting.MigrationFields, t *testing.T) (influxdbtesting.MigrationNSearchService, func()) {
	s, closeBolt, err := NewTestBoltStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initMigrationService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initInmemMigrationService(f influxdbtesting.MigrationFields, t *testing.T) (influxdbtesting.MigrationNSearchService, func()) {
	s, closeBolt, err := NewTestInmemStore()
	if err != nil {
		t.Fatalf("failed to create new kv store: %v", err)
	}

	svc, closeSvc := initMigrationService(s, f, t)
	return svc, func() {
		closeSvc()
		closeBolt()
	}
}

func initMigrationService(s kv.Store, f influxdbtesting.MigrationFields, t *testing.T) (influxdbtesting.MigrationNSearchService, func()) {
	svc := kv.NewService(s)
	svc.IDGenerator = f.IDGenerator

	ctx := context.Background()
	if err := svc.Initialize(ctx); err != nil {
		t.Fatalf("error initializing migration service: %v", err)
	}
	for _, o := range f.Organizations {
		if err := svc.PutOrganization(ctx, o); err != nil {
			t.Fatalf("failed to populate organizations")
		}
	}
	if err := svc.PutOldBuckets(ctx, f.OldBuckets); err != nil {
		t.Fatalf("failed to populate old buckets")
	}
	return svc, func() {
		for _, b := range f.OldBuckets {
			if err := svc.DeleteBucket(ctx, b.ID); err != nil {
				t.Logf("failed to remove buckets: %v", err)
			}
		}
		for _, o := range f.Organizations {
			if err := svc.DeleteOrganization(ctx, o.ID); err != nil {
				t.Logf("failed to remove organization: %v", err)
			}
		}
	}
}
