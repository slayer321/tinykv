package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	// ctx := context.WithValue(context.Background(), "key_to_check", req.Key)
	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		return &kvrpcpb.RawGetResponse{
			NotFound: true,
		}, err
	}
	return &kvrpcpb.RawGetResponse{
		Value:    value,
		NotFound: false,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modifieds

	putRequest := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	reqs := []storage.Modify{
		{Data: putRequest},
	}
	err := server.storage.Write(nil, reqs)
	if err != nil {
		log.Errorf("Returning error while writing to database %v\n", err)
	}
	return &kvrpcpb.RawPutResponse{
		Error: "No error",
	}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	deleteRequest := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}

	reqs := []storage.Modify{
		{Data: deleteRequest},
	}

	err := server.storage.Write(nil, reqs)

	if err != nil {
		log.Errorf("Returning error while writing to database %v\n", err)
	}
	return &kvrpcpb.RawDeleteResponse{
		Error: "No error",
	}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return nil, err
	}

	it := reader.IterCF(req.Cf)

	kvPairs := []*kvrpcpb.KvPair{}
	limitCount := 1

	for it.Seek(req.StartKey); it.Valid(); it.Next() {

		item := it.Item()

		valueCopy, err := item.ValueCopy(nil)
		if err != nil {
			log.Errorf("Not able to get the value for the key in scan %v\n", err)
		}

		kvPair := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: valueCopy,
		}

		kvPairs = append(kvPairs, kvPair)
		limitCount++

		if limitCount > int(req.Limit) {
			break
		}

	}

	return &kvrpcpb.RawScanResponse{
		Kvs: kvPairs,
	}, nil
}
