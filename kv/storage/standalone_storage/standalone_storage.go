package standalone_storage

import (
	"fmt"
	"os"

	//"github.com/dgraph-io/badger"
	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	dbPath string
	raft   bool
	db     *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		dbPath: conf.DBPath,
		raft:   false,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	err := os.MkdirAll(s.dbPath, os.ModePerm)
	if err != nil {
		log.Errorf("failed to create dir %v\n", s.dbPath)
		return err
	}

	opts := badger.DefaultOptions
	opts.Dir = s.dbPath
	opts.ValueDir = s.dbPath

	db, err := badger.Open(opts)
	if err != nil {
		log.Errorf("Failed to open the db %v\n", err)
		return err
	}
	s.db = db

	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).

	return StandAloneStorageReader{db: s.db}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batches []storage.Modify) error {
	// Your Code Here (1).
	var setKey string
	err := s.db.Update(func(txn *badger.Txn) error {
		var err error
		for _, batch := range batches {

			switch batch.Data.(type) {
			case storage.Put:
				setKey = fmt.Sprintf("%v_%v", batch.Cf(), string(batch.Key()))
				entry := &badger.Entry{}
				entry.Key = []byte(setKey)
				entry.Value = batch.Value()

				//e := badger.NewEntry([]byte(setKey), batch.Value())
				err = txn.SetEntry(entry)
				if err != nil {
					log.Errorf("Error while setting the keys and value %v\n", err)
				}
			case storage.Delete:
				deleteKey := fmt.Sprintf("%v_%v", batch.Cf(), string(batch.Key()))
				err = txn.Delete([]byte(deleteKey))
				if err != nil {
					log.Errorf("Error while deleting the key %v\n", err)
				}

			}

		}
		return err
	})
	if err != nil {
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	StandAloneStorage
	db *badger.DB
}

func (s StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var valueCopy []byte
	err := s.db.View(func(txn *badger.Txn) error {
		getKey := fmt.Sprintf("%v_%v", cf, string(key))
		item, err := txn.Get([]byte(getKey))
		if err != nil {
			return err
		}

		valueCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return valueCopy, nil
}

func (s StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {

	txn := s.db.NewTransaction(false)
	bi := engine_util.NewCFIterator(cf, txn)

	return bi
}

func (s StandAloneStorageReader) Close() {
	s.db.Close()

}
