package com.fdu.msacs.dfsmetanode.mongodb;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.util.List;

public interface FileNodeMappingRepository extends MongoRepository<FileNodes, String> {
    FileNodes findByFilename(String filename);
}