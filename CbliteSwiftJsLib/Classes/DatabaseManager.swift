//
//  DatabaseManager.swift
//  Created by Aaron LaBeau on 07/04/24.
//

import Foundation
import CouchbaseLiteSwift

enum DatabaseError: Error {
    case invalidDatabaseName(databaseName: String)
    case unableToOpenDatabase(databaseName: String)
    case unableToCloseDatabase(databaseName: String)
    case unableToDeleteDatabase(message: String, databaseName: String)
    case databaseLocked(databaseName: String)
    case copyError(message: String)
    case maintenanceError(message: String)
}

public class DatabaseManager {
    
    // MARK: - Private for management of state
    var openDatabases = [String: Database]()
    var queryResultSets = [String: ResultSet]()
    
    /* change listeners */
    var databaseChangeListeners = [String: Any]()
    var documentChangeListeners = [String: Any]()
    var queryChangeListeners = [String: Any]()
    
    /* replicators tracking */
    var replicators = [String: Replicator]()
    var replicatorChangeListeners = [String: Any]()
    var replicatorDocumentListeners = [String: Any]()
    
    var queryCount: Int = 0
    var replicatorCount: Int = 0
    var allResultsChunkSize: Int = 0
    
    /* collections */
    private var defaultCollectionName:String = "_default"
    private var defaultScopeName:String = "_default"
    
    // MARK: - Singleton
    static let shared = DatabaseManager()
    
    // MARK: - Private initializer to prevent external instantiation
    private init() {
        // Initialization code here
    }
    
    // MARK: - Helper methods
    
    public func getDatabase(_ name: String) -> Database? {
        objc_sync_enter(openDatabases)
        defer {
            objc_sync_exit(openDatabases)
        }
        
        return openDatabases[name]
    }
    
    public func buildDatabaseConfig(_ config: [AnyHashable: Any]?) -> DatabaseConfiguration {
        var databaseConfiguration = DatabaseConfiguration()
        if let encKey = config?["encryptionKey"] as? String {
            let key = EncryptionKey.password(encKey)
            databaseConfiguration.encryptionKey = key
        }
        if let directory = config?["directory"] as? String {
            // Used to auto set the database to be in the documents folder,
            // otherwise the directory won't work because we need a full path
            databaseConfiguration.directory = directory
        }
        return databaseConfiguration
    }
    
    // MARK: Database Methods
    
    public func open(_ databaseName: String, databaseConfig: [AnyHashable: Any]?) throws {
        do {
            let config = self.buildDatabaseConfig(databaseConfig)
            let database = try Database(name: databaseName, config: config)
            
            if self.openDatabases[databaseName] != nil {
                self.openDatabases.removeValue(forKey: databaseName)
            }
            self.openDatabases[databaseName] = database
        } catch {
            throw DatabaseError.unableToOpenDatabase(databaseName: databaseName)
        }
    }
    
    public func close(_ databaseName: String) throws {
        guard let database = self.getDatabase(databaseName) else {
            throw DatabaseError.invalidDatabaseName(databaseName: databaseName)
        }
        do {
            try database.close()
        } catch {
            throw DatabaseError.unableToCloseDatabase(databaseName: databaseName)
        }
    }
    
    func delete(_ databaseName: String) throws {
        guard let database = self.getDatabase(databaseName) else {
            throw DatabaseError.invalidDatabaseName(databaseName: databaseName)
        }
        do {
            try database.delete()
            openDatabases.removeValue(forKey: databaseName)
        } catch {
            if let nsError = error as NSError?, nsError.code == 19 {
                // SQLite error code 19 (SQLITE_CONSTRAINT) indicates that the database is locked.
                throw DatabaseError.databaseLocked(databaseName: databaseName)
            } else {
                throw DatabaseError.unableToDeleteDatabase(message: "Error deleting database: \(error.localizedDescription)", databaseName: databaseName)
            }
        }
    }
    
    public func exists(_ databaseName: String, directoryPath: String) -> Bool {
        return Database.exists(withName: databaseName, inDirectory: directoryPath)
    }
    
    public func getPath(_ databaseName: String) throws -> String? {
        guard let database = self.getDatabase(databaseName) else {
            throw DatabaseError.invalidDatabaseName(databaseName: databaseName)
        }
        return database.path
    }
    
    public func copy(_ path: String, newName: String, databaseConfig: [AnyHashable: Any]?) throws {
        let config = self.buildDatabaseConfig(databaseConfig)
        do {
            try Database.copy(fromPath: path, toDatabase: newName, withConfig: config)
        } catch {
            throw DatabaseError.copyError(message: "\(error.localizedDescription)")
        }
    }
    
    // MARK: Database Maintenance methods
    
    func performMaintenance(_ databaseName: String, maintenanceType: MaintenanceType) throws {
        guard let database = self.getDatabase(databaseName) else {
            throw DatabaseError.invalidDatabaseName(databaseName: databaseName)
        }
        
        do {
            try database.performMaintenance(type: maintenanceType)
        } catch {
            if let nsError = error as NSError? {
                let errorMessage: String
                if let reason = nsError.userInfo[NSLocalizedFailureReasonErrorKey] as? String {
                    throw DatabaseError.maintenanceError(message:  "Unknown error: \(reason)")
                }
            }
            throw DatabaseError.maintenanceError(message: "Unknown error trying to perform maintenance \(error)")
        }
    }
    
    // MARK: Index Methods
    
    func createIndex(_ indexName: String,
                        indexType: String,
                        items: [[Any]],
                        databaseName: String) throws {
        do {
            try CollectionManager.shared.createIndex(indexName, indexType: indexType, 
                items: items, 
                collectionName: defaultCollectionName,
                scopeName: defaultScopeName,
                databaseName: databaseName)
        }
        catch {
            throw error
        }
    }
    
    func deleteIndex(_ indexName: String,
                        indexType: String,
                        items: [[Any]],
                        databaseName: String) throws {
        do {
            try CollectionManager.shared.deleteIndex(indexName,
                collectionName: defaultCollectionName,
                scopeName: defaultScopeName,
                databaseName: databaseName)
        }
        catch {
            throw error
        }
    }
    
    func getIndexes(databaseName: String) throws -> [String] {
        do {
            let indexes = try CollectionManager.shared.indexes(
                defaultCollectionName,
                scopeName: defaultScopeName,
                databaseName: databaseName)
            return indexes
        } catch {
            throw error
        }
    }
    
    // MARK: Document methods
   
    func getDocumentsCount(_ databaseName: String) 
        throws -> UInt64 {
        
            do {
                return try CollectionManager.shared.documentsCount(
                    defaultCollectionName,
                    scopeName: defaultScopeName,
                    databaseName: databaseName)
            } catch {
                throw error
            }
    }
    
    func saveDocument(_ documentId: String,
                      document: [String: Any],
                      concurrencyControl: ConcurrencyControl?,
                      collectionName: String?,
                      scopeName: String?,
                      databaseName: String) throws -> String {
        do {
            return try CollectionManager.shared.saveDocument(documentId, document: document, concurrencyControl: concurrencyControl, collectionName: defaultCollectionName, scopeName: defaultScopeName, databaseName: databaseName)
        } catch {
            throw error
        }
    }
    
    func getDocument(_ documentId: String,
                     databaseName: String) throws -> Document? {
        do {
            return try CollectionManager.shared.document(
                documentId,
                collectionName: defaultCollectionName,
                scopeName: defaultScopeName,
                databaseName: databaseName)
        }
        catch {
            throw error
        }
    }
    
    func deleteDocument(_ documentId: String,
                        databaseName: String) throws {
        do {
            try CollectionManager.shared.deleteDocument(
                documentId, 
                collectionName: defaultCollectionName,
                scopeName: defaultScopeName,
                databaseName: databaseName)
        } catch {
            throw error
        }
    }
    
    func deleteDocument(_ documentId: String,
                        concurrencyControl: ConcurrencyControl,
                        databaseName: String) throws -> String {
        do {
            return try CollectionManager.shared.deleteDocument(
                documentId,
                concurrencyControl: concurrencyControl,
                collectionName: defaultCollectionName,
                scopeName: defaultScopeName,
                databaseName: databaseName)
        } catch {
            throw error
        }
    }
    
    func purgeDocument(_ documentId: String,
                       databaseName: String) throws {
        do {
            try CollectionManager.shared.purgeDocument(
                documentId,
                collectionName: defaultCollectionName,
                scopeName: defaultScopeName,
                databaseName: databaseName)
        } catch {
            throw error
        }
    }
   
    
    
}
