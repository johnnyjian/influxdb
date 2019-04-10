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
