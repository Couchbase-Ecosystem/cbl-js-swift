//
//  ReplicatorManager.swift
//  CbliteSwiftJsLib
//

import Foundation
import CouchbaseLiteSwift

enum ReplicatorError: Error {
    case configurationError(message: String)
    case unableToFindReplicator(replicatorId: String)
    case unknownError(message: String)
    case fatalError(message: String)
    case invalidState(message: String)
}

public class ReplicatorManager {

    /* replicators tracking */
    var replicators = [String: Replicator]()
    var replicatorChangeListeners = [String: Any]()
    var replicatorDocumentListeners = [String: Any]()

    // MARK: - Singleton
    public static let shared = ReplicatorManager()

    // MARK: - Private initializer to prevent external instatiation
    private init() { }

    // MARK: - Helper Functions
    public func getReplicator(replicatorId: String) -> Replicator? {
        return self.replicators[replicatorId]
    }
    
    public func removeReplicator(replicatorId: String) {
        self.replicators.removeValue(forKey: replicatorId)
    }

    // MARK: Replicator Functions

    /// **[DUAL API SUPPORT]** Creates a replicator instance from configuration
    ///
    /// **What it does:**
    /// - Receives JSON string with collection configuration (NEW or OLD format)
    /// - Automatically detects which API format is being used
    /// - Creates a new replicator with appropriate collection configuration
    /// - Generates a unique ID for tracking the replicator
    /// - Stores the replicator in the manager's registry
    ///
    /// **Parameters:**
    /// - `replicatorConfig`: Dictionary containing all replicator settings
    /// - `collectionConfigJson`: JSON string in NEW or OLD format
    ///
    /// **NEW API Format Example:**
    /// ```json
    /// "[{\"collection\":{\"name\":\"users\",\"scopeName\":\"_default\",\"databaseName\":\"mydb\"},\"config\":{\"channels\":[\"public\"]}}]"
    /// ```
    ///
    /// **OLD API Format Example:**
    /// ```json
    /// "[{\"collections\":[{\"collection\":{\"name\":\"users\",\"scopeName\":\"_default\",\"databaseName\":\"mydb\"}}],\"config\":{\"channels\":[\"public\"]}}]"
    /// ```
    ///
    /// **Returns:**
    /// - `String`: Unique replicator ID for future operations
    ///
    /// **Example return value:**
    /// ```swift
    /// "A3B4C5D6-E7F8-4A9B-8C7D-1E2F3A4B5C6D"
    /// ```
    ///
    /// **Throws:**
    /// - `ReplicatorError`: If configuration is invalid or JSON cannot be parsed
    /// - `CollectionError`: If any collection doesn't exist
    public func replicator(
        _ replicatorConfig: [String: Any],
        collectionConfigJson: String
    ) throws -> String {
        print("\n╔═══════════════════════════════════════════════════════════════╗")
        print("║  [ReplicatorManager] CREATING REPLICATOR INSTANCE             ║")
        print("╚═══════════════════════════════════════════════════════════════╝")
        
        print("[ReplicatorManager Step 1] Generating unique ID...")
        let id = UUID().uuidString
        print("[ReplicatorManager Step 1] ✅ ID generated: \(id)")
        
        print("[ReplicatorManager Step 2] Creating ReplicatorConfiguration...")
        let config = try ReplicatorHelper.replicatorConfigFromJson(
            replicatorConfig,
            collectionConfigJson: collectionConfigJson
        )
        print("[ReplicatorManager Step 2] ✅ ReplicatorConfiguration created")
        
        print("[ReplicatorManager Step 3] Creating Replicator instance...")
        do {
            let replicator = Replicator(config: config)
            print("[ReplicatorManager Step 3] ✅ Replicator instance created")
            
            print("[ReplicatorManager Step 4] Storing replicator in registry...")
            replicators[id] = replicator
            print("[ReplicatorManager Step 4] ✅ Replicator stored, total count: \(replicators.count)")
            
            print("\n╔═══════════════════════════════════════════════════════════════╗")
            print("║  [ReplicatorManager] ✅ REPLICATOR INSTANCE CREATED           ║")
            print("╚═══════════════════════════════════════════════════════════════╝\n")
            
            return id
        } catch {
            print("[ReplicatorManager Step 3] ❌ FAILED: \(error)")
            print("[ReplicatorManager Step 3] Error type: \(type(of: error))")
            throw error
        }
    }

    public func start(_ replicatorId: String) throws {
        print("\n[ReplicatorManager.start] Attempting to start replicator: \(replicatorId)")
        if let replicator = getReplicator(replicatorId: replicatorId) {
            print("[ReplicatorManager.start] ✅ Replicator found, starting...")
            replicator.start()
            print("[ReplicatorManager.start] ✅ Replicator started")
        } else {
            print("[ReplicatorManager.start] ❌ FAILED: Replicator not found")
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
    }

    public func stop(_ replicatorId: String) throws {
        if let replicator = getReplicator(replicatorId: replicatorId) {
            replicator.stop()
        } else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
    }

    public func resetCheckpoint(_ replicatorId: String) throws {
        if let replicator = getReplicator(replicatorId: replicatorId) {
            let status = replicator.status
            let activity = status.activity
            if activity == .stopped || activity == .idle {
                replicator.start(reset: true)
            } else {
                throw ReplicatorError.invalidState(message: "replicator is in an invalid state to reset checkpoint: \(activity)")
            }
        } else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
    }

    public func getStatus(_ replicatorId: String) throws -> [String: Any] {
        if let replicator = getReplicator(replicatorId: replicatorId) {
            let status = replicator.status
            let statusJson = ReplicatorHelper.generateReplicatorStatusJson(status)
            return statusJson
        } else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
    }
    
    public func getPendingDocumentIds(_ replicatorId: String, collection: Collection) throws ->  [String:Any] {
        if let replicator = getReplicator(replicatorId: replicatorId) {
            do {
                let documentIds = try replicator.pendingDocumentIds(collection: collection)
                let stringArray = Array(documentIds)
                return ["pendingDocumentIds": stringArray];
            } catch {
                throw error
            }
        } else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
    }
    
    public func isDocumentPending(_ replicatorId: String, documentId: String, collection: Collection) throws ->  [String:Any] {
        if let replicator = getReplicator(replicatorId: replicatorId) {
            do {
                let isPending = try replicator.isDocumentPending(documentId, collection: collection)
                return ["isPending": isPending];
            } catch {
                throw error
            }
        } else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
    }

    public func cleanUp(_ replicatorId: String) throws {
        if let replicator = getReplicator(replicatorId: replicatorId) {
            replicator.stop()
            self.replicators.removeValue(forKey: replicatorId)
        } else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
    }
    
    public func pendingDocIds(_ replicatorId: String, collectionName: String, scopeName: String, databaseName: String) throws -> Set<String> {
        guard let collection = try CollectionManager.shared.getCollection(collectionName, scopeName: scopeName, databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
        }
        guard let replicator = self.getReplicator(replicatorId: replicatorId) else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
        do {
            let docIds = try replicator.pendingDocumentIds(collection: collection)
            return docIds
        } catch {
            throw error
        }
    }
    
    public func isDocumentPending(_ documentId: String, replicatorId: String, collectionName: String, scopeName: String, databaseName: String) throws -> Bool {
        guard let collection = try CollectionManager.shared.getCollection(collectionName, scopeName: scopeName, databaseName: databaseName) else {
            throw CollectionError.unableToFindCollection(collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
        }
        guard let replicator = self.getReplicator(replicatorId: replicatorId) else {
            throw ReplicatorError.unableToFindReplicator(replicatorId: replicatorId)
        }
        do {
            let isPending = try replicator.isDocumentPending(documentId, collection: collection)
            return isPending
        } catch {
            throw error
        }
    }
}
