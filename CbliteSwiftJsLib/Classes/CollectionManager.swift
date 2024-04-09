//
//  CollectionManager.swift
//  CbliteSwiftJsLib
//
//  Created by Aaron LaBeau on 07/04/24.
//

import Foundation
import CouchbaseLiteSwift

enum CollectionError: Error {
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
    var collections = [String: Collection]()
    var documentChangeListeners = [String: Any]()

    // MARK: - Singleton
    static let shared = CollectionManager()

    // MARK: - Private initializer to prevent external instatiation
    private init() {

    }

    // MARK: - Helper Functions

    public func getCollectionKey(_ collectionName: String,
                                  scopeName: String,
                                  databaseName: String) -> String {
        return "\(databaseName).\(scopeName).\(collectionName)"
    }

    public func getCollection(_ collectionName: String,
                              scopeName: String,
                              databaseName: String) throws -> Collection? {
        guard let database = DatabaseManager.shared.getDatabase(databaseName) else {
            throw DatabaseError.invalidDatabaseName(databaseName: databaseName)
        }

        do {
            let key = getCollectionKey(
                collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
            if self.collections[key] != nil {
                return self.collections[key]
            } else {
                guard let collection = try database.collection(
                    name: collectionName,
                    scope: scopeName) else {
                    throw CollectionError.unableToFindCollection(collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
                }
                self.collections[key] = collection
                return collection
            }
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

    func deleteIndex(_ indexName: String,
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

    func indexes(_ collectionName: String,
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

    func documentsCount(_ collectionName: String,
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

    func saveDocument(_ documentId: String,
                      document: [String: Any],
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
        let mutableDocument: MutableDocument
        if !documentId.isEmpty {
            mutableDocument = MutableDocument(id: documentId, data: MapHelper.toMap(document))
        } else {
            mutableDocument = MutableDocument(data: MapHelper.toMap(document))
        }

        do {

            if let concurrencyControlValue = concurrencyControl {
                let results = try collection.save(document: mutableDocument, concurrencyControl: concurrencyControlValue)
                if results {
                    return "true"
                }
                return "false"
            } else {
                try collection.save(document: mutableDocument)
                return documentId
            }
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }

    func document(_ documentId: String,
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

    func getBlobContent(_ key: String, documentId: String, collectionName: String, scopeName: String, databaseName: String) throws -> [Int]? {
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

    func deleteDocument(_ documentId: String,
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

    func deleteDocument(_ documentId: String,
                        concurrencyControl: ConcurrencyControl,
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
            let result = try collection.delete(document: document, concurrencyControl: concurrencyControl)
            if result {
                return "true"
            }
            return "false"
        } catch {
            throw CollectionError.documentError(
                message: error.localizedDescription,
                collectionName: collectionName,
                scopeName: scopeName,
                databaseName: databaseName)
        }
    }

    func purgeDocument(_ documentId: String,
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

    func setDocumentExpiration(_ documentId: String,
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

    func getDocumentExpiration(_ documentId: String,
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
}
