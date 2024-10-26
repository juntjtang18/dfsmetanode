package com.infolink.dfs.metanode.mdb;

import java.util.Optional;

import org.springframework.data.mongodb.repository.MongoRepository;

public interface FileNodeMappingRepo extends MongoRepository<FileNodeMapping, String> {
	Optional<FileNodeMapping> findByFilename(String filename);
}
