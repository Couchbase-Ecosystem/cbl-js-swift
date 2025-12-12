//
//  CollectionManager+Vector.swift
//  CbliteSwiftJsLib
//
//  Vector index support extension for CollectionManager.
//  This file shows the changes needed to add vector search support.
//
//  Add this case to the createIndex switch statement in CollectionManager.swift
//

import Foundation
import CouchbaseLiteSwift

// MARK: - Vector Index Extension

extension CollectionManager {
    
    /// Creates a vector index on the specified collection.
    /// 
    /// This method should be integrated into the main createIndex() method
    /// as an additional case in the switch statement.
    ///
    /// - Parameters:
    ///   - indexName: Name for the new index
    ///   - indexConfig: Dictionary containing vector index configuration
    ///   - collection: The collection to create the index on
    /// - Throws: CollectionError if index creation fails
    public func createVectorIndex(
        _ indexName: String,
        indexConfig: [String: Any],
        collection: Collection
    ) throws {
        // Extract required parameters
        guard let expression = indexConfig["expression"] as? String else {
            throw CollectionError.cannotCreateIndex(indexName: indexName)
        }
        guard let dimensions = indexConfig["dimensions"] as? UInt32 else {
            throw CollectionError.cannotCreateIndex(indexName: indexName)
        }
        guard let centroids = indexConfig["centroids"] as? UInt32 else {
            throw CollectionError.cannotCreateIndex(indexName: indexName)
        }
        
        // Create vector index configuration
        let vectorConfig = VectorIndexConfiguration(
            expression: expression,
            dimensions: dimensions,
            centroids: centroids
        )
        
        // Set distance metric
        if let metricStr = indexConfig["metric"] as? String {
            switch metricStr {
            case "cosine":
                vectorConfig.metric = .cosine
            case "euclidean":
                vectorConfig.metric = .euclidean
            case "euclideanSquared":
                vectorConfig.metric = .euclideanSquared
            case "dot":
                vectorConfig.metric = .dot
            default:
                // Default to euclideanSquared (fastest)
                vectorConfig.metric = .euclideanSquared
            }
        }
        
        // Set encoding
        if let encodingDict = indexConfig["encoding"] as? [String: Any],
           let encodingType = encodingDict["type"] as? String {
            switch encodingType {
            case "none":
                vectorConfig.encoding = .none
            case "SQ":
                // Scalar Quantizer - 8-bit by default
                vectorConfig.encoding = .scalarQuantizer(type: .SQ8)
            case "PQ":
                // Product Quantizer
                if let subquantizers = encodingDict["subquantizers"] as? UInt32,
                   let bits = encodingDict["bits"] as? UInt32 {
                    vectorConfig.encoding = .productQuantizer(subquantizers: subquantizers, bits: bits)
                } else {
                    vectorConfig.encoding = .none
                }
            default:
                vectorConfig.encoding = .none
            }
        }
        
        // Set optional training parameters
        if let minTraining = indexConfig["minTrainingSize"] as? UInt32, minTraining > 0 {
            vectorConfig.minTrainingSize = minTraining
        }
        
        if let maxTraining = indexConfig["maxTrainingSize"] as? UInt32, maxTraining > 0 {
            vectorConfig.maxTrainingSize = maxTraining
        }
        
        // Set number of probes (affects search accuracy vs speed)
        if let numProbes = indexConfig["numProbes"] as? UInt32, numProbes > 0 {
            vectorConfig.numProbes = numProbes
        }
        
        // Set lazy indexing (index built on first query)
        if let isLazy = indexConfig["isLazy"] as? Bool {
            vectorConfig.isLazy = isLazy
        }
        
        // Create the index
        do {
            try collection.createIndex(withName: indexName, config: vectorConfig)
        } catch {
            throw CollectionError.createIndex(
                indexName: indexName,
                message: error.localizedDescription
            )
        }
    }
}

// MARK: - Integration Instructions
/*
 
 To integrate this into CollectionManager.swift, modify the createIndex() method:
 
 1. Add an optional indexConfig parameter:
 
    public func createIndex(_ indexName: String,
                            indexType: String,
                            items: [[Any]],
                            indexConfig: [String: Any]? = nil,  // ADD THIS
                            collectionName: String,
                            scopeName: String,
                            databaseName: String) throws {
 
 2. Add the vector case to the switch statement:
 
    switch indexType {
    case "value":
        // existing code...
        
    case "full-text":
        // existing code...
        
    case "vector":
        guard let config = indexConfig else {
            throw CollectionError.cannotCreateIndex(indexName: indexName)
        }
        try self.createVectorIndex(indexName, indexConfig: config, collection: collection)
        
    default:
        throw CollectionError.unknownIndexType(indexType: indexType)
    }
 
 3. Update the CblReactnative.swift bridge to pass the full config dictionary
    for vector indexes.
 
*/
