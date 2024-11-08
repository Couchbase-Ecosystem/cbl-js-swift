//
//  CollectionManager.swift
//  CbliteSwiftJsLib
//

import Foundation
import CouchbaseLiteSwift

public struct CollectionConfigItem: Codable {
    let collections: [CollectionDtoWrapper]
    let config: ConfigDto
}

public struct CollectionDtoWrapper: Codable {
    let collection: CollectionDto
}

public struct CollectionDto: Codable {
    let name: String
    let scopeName: String
    let databaseName: String
}

public struct ConfigDto: Codable {
    let channels: [String]
    let documentIds: [String]
}

public struct DocumentDto: Codable {
    let document: String
    let blobs: String
}

public struct CollectionDocumentResult {
    let id: String
    let revId: String?
    let sequence: UInt64
    let concurrencyControl: Bool?
}

public enum CollectionError: Error {
    case unableToFindCollection(collectionName: String, scopeName: String, databaseName: String)
    case getCollection(message: String, collectionName: String, scopeName: String, databaseName: String)
    case cannotCreateIndex(indexName: String)
    case createIndex(indexName: String, message: String)
    case unknownIndexType(indexType: String)
    case documentError(message: String, collectionName: String, scopeName: String, databaseName: String)
}

public class CollectionManager {
    
    private var defaultCollectionName: String = "_default"
    private var defaultScopeName: String = "_default"
    
    // MARK: - Private for management of state
    
    // index is based on databaseName.scopeName.collectionName
    var documentChangeListeners = [String: Any]()
    
    // MARK: - Singleton
    public static let shared = CollectionManager()
    
    // MARK: - Private initializer to prevent external instatiation
    private init() {
        
    }
    
    // MARK: - Helper Functions
    
    public func blobsFromJsonString(_ value: String) throws -> [String: Blob] {
        var blobs = [String: Blob]()
        if (value.isEmpty || value == "[]") {
            return blobs
        }
        do {
            if let data = value.data(using: .utf8) {
                if let map = try JSONSerialization.jsonObject(with: data, options: []) as? [String: Any] {
                    for (key, value) in map {
                        if let object = value as? [String: Any],
                           let type = object["_type"] as? String, type == "blob" {
                            if let blobData = object["data"] as? [String: Any],
                               let contentType = blobData["contentType"] as? String,
                               let bytes = blobData["data"] as? [NSNumber] {
                                
                                var bytesCArray = [UInt8](repeating: 0, count: bytes.count)
                                for (index, byte) in bytes.enumerated() {
                                    bytesCArray[index] = byte.uint8Value
                                }
                                
                                let data = Data(bytesCArray)
                                let blob = Blob(contentType: contentType, data: data)
                                blobs[key] = blob
                                continue
                            }
                        }
                    }
                }
            }
        } catch {
           throw error
        }
        return blobs
    }
    
    public func getCollection(_ collectionName: String,
                              scopeName: String,
                              databaseName: String) throws -> Collection? {
        guard let database = DatabaseManager.shared.getDatabase(databaseName) else {
            throw DatabaseError.invalidDatabaseName(databaseName: databaseName)
        }
        
        do {
            guard let collection = try database.collection(
                name: collectionName,
                scope: scopeName) else {
                throw CollectionError.unableToFindCollection(collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
            }
            return collection
        } catch {
            throw CollectionError.getCollection(message: error.localizedDescription, collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
        }
    }
    
    // MARK: Index Functions
    
    public func createIndex(_ indexName: String,
                            indexType: String,
                            items: [[Any]],
                            collectionName: String,
                            scopeName: String,
                            databaseName: String) throws {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
        }
        
        let index: Index
        switch indexType {
            case "value":
                index = IndexBuilder.valueIndex(items: IndexHelper.makeValueIndexItems(items))
            case "full-text":
                index = IndexBuilder.fullTextIndex(items: IndexHelper.makeFullTextIndexItems(items))
            default:
                throw CollectionError.unknownIndexType(indexType: indexType)
        }
        
        do {
            try collection.createIndex(index, name: indexName)
        } catch {
            throw CollectionError.createIndex(indexName: indexName, message: error.localizedDescription)
        }
    }
    
    public func deleteIndex(_ indexName: String,
                            collectionName: String,
                            scopeName: String,
                            databaseName: String) throws {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        do {
            try collection.deleteIndex(forName: indexName)
        } catch {
            throw error
        }
    }
    
    public func indexes(_ collectionName: String,
                        scopeName: String,
                        databaseName: String) throws -> [String] {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName),
              let indexes = try? collection.indexes() else {
            throw CollectionError.unableToFindCollection(collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
        }
        return indexes
    }
    
    // MARK: Document Functions
    
    public func deleteDocument(_ documentId: String,
                               collectionName: String,
                               scopeName: String,
                               databaseName: String) throws {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        do {
            guard let document =  try collection.document(id: documentId) else {
                throw CollectionError.documentError(
                    message: "Document not found with id: \(documentId)",
                    collectionName: collectionName,
                    scopeName: scopeName,
                    databaseName: databaseName)
            }
            try collection.delete(document: document)
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }
    
    public func deleteDocument(_ documentId: String,
                               concurrencyControl: ConcurrencyControl?,
                               collectionName: String,
                               scopeName: String,
                               databaseName: String) throws -> String {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        do {
            guard let document =  try collection.document(id: documentId) else {
                throw CollectionError.documentError(
                    message: "Document not found with id: \(documentId)",
                    collectionName: collectionName,
                    scopeName: scopeName,
                    databaseName: databaseName)
            }
            if let cc = concurrencyControl {
                let result = try collection.delete(document: document, concurrencyControl: cc)
                if result {
                    return "true"
                }
                return "false"
            } else {
                try collection.delete(document: document)
                return ""
            }
            
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }
    
    public func document(_ documentId: String,
                         collectionName: String,
                         scopeName: String,
                         databaseName: String) throws -> Document? {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        do {
            let document =  try collection.document(id: documentId)
            return document
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }
    
    public func documentsCount(_ collectionName: String,
                               scopeName: String,
                               databaseName: String) throws -> UInt64 {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        return collection.count
    }
    
    public func getDocumentExpiration(_ documentId: String,
                                      collectionName: String,
                                      scopeName: String,
                                      databaseName: String) throws -> Date? {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        do {
            let result = try collection.getDocumentExpiration(id: documentId)
            return result
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        
    }
    
    public func getBlobContent(_ key: String,
                               documentId: String,
                               collectionName: String,
                               scopeName: String,
                               databaseName: String) throws -> [Int]? {
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        
        guard let document = try collection.document(id: documentId) else {
            throw CollectionError.documentError(message: "can't find document", collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
        }
        
        guard let blob = document.blob(forKey: key) else {
            return []
        }
        
        if let data = blob.content {
            var content: [Int] = []
            data.regions.forEach { region in
                for byte in region {
                    content.append(Int(byte))
                }
            }
            return content
        }
        return []
    }
    
    public func purgeDocument(_ documentId: String,
                              collectionName: String,
                              scopeName: String,
                              databaseName: String) throws {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        do {
            try collection.purge(id: documentId)
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }
    
    public func saveDocument(_ documentId: String,
                             document: String,
                             blobs: [String: Blob],
                             concurrencyControl: ConcurrencyControl?,
                             collectionName: String,
                             scopeName: String,
                             databaseName: String) throws -> CollectionDocumentResult {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    
        do {
            //create the document
            let mutableDocument: MutableDocument
            if !documentId.isEmpty {
                mutableDocument = try MutableDocument(id: documentId, json: document)
            } else {
                mutableDocument = try MutableDocument(json: document)
            }

            //update the document with the blobs
            for (key, blob) in blobs {
                mutableDocument.setBlob(blob, forKey: key)
            }
            
            if let concurrencyControlValue = concurrencyControl {
                let results = try collection.save(document: mutableDocument, concurrencyControl: concurrencyControlValue)
                if results {
                    return CollectionDocumentResult(
                        id: mutableDocument.id,
                        revId:  mutableDocument.revisionID,
                        sequence: mutableDocument.sequence,
                        concurrencyControl: true)
                    
                }
                return CollectionDocumentResult(
                    id: mutableDocument.id,
                    revId:  mutableDocument.revisionID,
                    sequence: mutableDocument.sequence,
                    concurrencyControl: false)
            } else {
                try collection.save(document: mutableDocument)
                return CollectionDocumentResult(
                    id: mutableDocument.id,
                    revId:  mutableDocument.revisionID,
                    sequence: mutableDocument.sequence,
                    concurrencyControl: nil)
            }
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }
    
    public func setDocumentExpiration(_ documentId: String,
                                      expiration: Date?,
                                      collectionName: String,
                                      scopeName: String,
                                      databaseName: String) throws {
        
        guard let collection = try self.getCollection(
            collectionName,
            scopeName: scopeName,
            databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
        do {
            try collection.setDocumentExpiration(
                id: documentId,
                expiration: expiration)
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }
}
