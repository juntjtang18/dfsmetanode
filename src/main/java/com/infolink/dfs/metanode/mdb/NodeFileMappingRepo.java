package com.infolink.dfs.metanode.mdb;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface NodeFileMappingRepo extends MongoRepository<NodeFileMapping, String> {
	Optional<NodeFileMapping> findByNodeUrl(String nodeUrl);
}
