package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/influxdb"
)

var (
	migrationK = []byte("result")
	migrationV = []byte{0x1}
)

var _ influxdb.DataMigrationService = (*Service)(nil)

// CheckAndMigrate will check all applicable migration and perform the migration.
func (s *Service) CheckAndMigrate(ctx context.Context) (err error) {
	if !s.IsBucketMigrated(ctx) {
		if err = s.ConvertBucketToNew(ctx); err != nil {
			return err
		}
	}
	return nil
}

// IsBucketMigrated will determine if data already migrated.
func (s *Service) IsBucketMigrated(ctx context.Context) bool {
	if err := s.kv.View(ctx, func(tx Tx) error {
		b, err := tx.Bucket(influxdb.BucketIsMigratedIndex)
		if err != nil {
			return err
		}
		v, err := b.Get(migrationK)
		if err != nil {
			return err
		}
		if string(migrationV) != string(v) {
			return &influxdb.Error{
				Msg: "unexpected error bucket conversion error",
			}
		}
		return nil
	}); err != nil {
		return false
	}
	return true
}

// ConvertBucketToNew to do a scan to the storage and convert every thing related.
func (s *Service) ConvertBucketToNew(ctx context.Context) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		bkt, err := s.bucketsBucket(tx)
		if err != nil {
			return err
		}

		cur, err := bkt.Cursor()
		if err != nil {
			return err
		}
		k, v := cur.First()
		for k != nil {
			old := &influxdb.OldBucket{}
			if err := json.Unmarshal(v, old); err != nil {
				return &influxdb.Error{
					Err: err,
					Msg: fmt.Sprintf("unprocessable old bucket: %s", string(v)),
				}
			}
			b := influxdb.ConvertOldBucketToNew(*old)
			s.putBucket(ctx, tx, &b)
			k, v = cur.Next()
		}
		index, err := tx.Bucket(influxdb.BucketIsMigratedIndex)
		if err != nil {
			return UnexpectedBucketError(err)
		}
		return index.Put(migrationK, migrationV)
	})
}

// PutOldBuckets is for testing migration only.
func (s *Service) PutOldBuckets(ctx context.Context, bs []influxdb.OldBucket) error {
	return s.kv.Update(ctx, func(tx Tx) error {
		for _, b := range bs {
			if err := s.putOldBucket(ctx, tx, b); err != nil {
				return err
			}
		}
		return nil
	})
}

// putOldBucket is for testing migration only.
func (s *Service) putOldBucket(ctx context.Context, tx Tx, b influxdb.OldBucket) error {
	v, err := json.Marshal(b)
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	encodedID, err := b.ID.Encode()
	if err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	bkt, err := s.bucketsBucket(tx)
	if bkt.Put(encodedID, v); err != nil {
		return &influxdb.Error{
			Err: err,
		}
	}
	return nil
}
