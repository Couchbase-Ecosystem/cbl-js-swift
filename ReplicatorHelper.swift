//
//  ReplicatorHelper.swift
//  CbliteSwiftJsLib
//

import Foundation
import JavaScriptCore
import CouchbaseLiteSwift

public struct ReplicatorHelper {
    private static let jsContextQueue = DispatchQueue(label: "com.couchbase.jscontext", attributes: .concurrent)
    private static var filterCallCount = 0
    private static var lastFilterError: String?
    
    private static let jsContext: JSContext = {
        let context = JSContext()!
        
        // Enhanced logging function
        let logFunction: @convention(block) (String) -> Void = { message in
            print("🔵 JSFilter: \(message)")
        }
        
        context.setObject(logFunction, forKeyedSubscript: "log" as NSString)
        context.evaluateScript("var console = { log: log };")
        
        // Handle JavaScript exceptions with more detail
        context.exceptionHandler = { context, exception in
            if let exc = exception {
                let errorMsg = "❌ JSFilter Exception: \(exc.toString() ?? "unknown error")"
                print(errorMsg)
                lastFilterError = errorMsg
            }
        }
        
        return context
    }()
    
    private static func evaluateFilter(_ filterFunction: String, document: Document, flags: DocumentFlags) -> Bool {
        let filterID = filterCallCount
        filterCallCount += 1
        
        print("🟢 Filter[\(filterID)] START - DocID: \(document.id)")
//  guard let replicator = ReplicatorManager.shared.replicators.values.first,
//           replicator.status.activity != .stopped else {
//         print("⚠️ Filter[\(filterID)] Replicator no longer active, allowing document")
//         return true  // Allow document if replicator is gone
//     }
        
        return jsContextQueue.sync {
            do {
                // Log document size
                let docDict = document.toDictionary()
                var fullDocDict = docDict
                fullDocDict["_id"] = document.id
                
                print("🔍 Filter[\(filterID)] Document keys: \(fullDocDict.keys.joined(separator: ", "))")
                
                // Convert to JSON string with proper escaping
                guard let docData = try? JSONSerialization.data(withJSONObject: fullDocDict, options: []) else {
                    print("❌ Filter[\(filterID)] Failed to serialize document")
                    return false
                }
                
                let docSize = docData.count
                print("📊 Filter[\(filterID)] Document size: \(docSize) bytes")
                
                guard let docJsonRaw = String(data: docData, encoding: .utf8) else {
                    print("❌ Filter[\(filterID)] Failed to convert document to string")
                    return false
                }
                
                // Log if document is particularly large
                if docSize > 10000 {
                    print("⚠️ Filter[\(filterID)] Large document detected: \(docSize) bytes")
                }
                
                // Escape the JSON string for JavaScript
                let docJson = docJsonRaw
                    .replacingOccurrences(of: "\\", with: "\\\\")
                    .replacingOccurrences(of: "'", with: "\\'")
                    .replacingOccurrences(of: "\n", with: "\\n")
                    .replacingOccurrences(of: "\r", with: "\\r")
                    .replacingOccurrences(of: "\t", with: "\\t")
                
                // Create flags object
                let flagsDict: [String: Bool] = [
                    "deleted": flags.contains(.deleted),
                    "accessRemoved": flags.contains(.accessRemoved)
                ]
                
                print("🏳️ Filter[\(filterID)] Flags: \(flagsDict)")
                
                guard let flagsData = try? JSONSerialization.data(withJSONObject: flagsDict, options: []),
                      let flagsJsonRaw = String(data: flagsData, encoding: .utf8) else {
                    print("❌ Filter[\(filterID)] Failed to serialize flags")
                    return false
                }
                
                let flagsJson = flagsJsonRaw
                    .replacingOccurrences(of: "\\", with: "\\\\")
                    .replacingOccurrences(of: "'", with: "\\'")
                
                // Create and execute the filter script
                let script = """
                (function() {
                    try {
                        console.log('Filter[\(filterID)] Parsing document...');
                        const filterFunc = \(filterFunction);
                        const doc = JSON.parse('\(docJson)');
                        const flags = JSON.parse('\(flagsJson)');
                        
                        console.log('Filter[\(filterID)] Document ID: ' + doc._id);
                        console.log('Filter[\(filterID)] Calling filter function...');
                        
                        // Call the filter function
                        const result = filterFunc(doc, flags);
                        
                        console.log('Filter[\(filterID)] Filter returned: ' + result);
                        
                        // Ensure we return a boolean
                        return !!result;
                    } catch (e) {
                        console.log('Filter[\(filterID)] ERROR: ' + e.toString());
                        console.log('Filter[\(filterID)] Stack: ' + (e.stack || 'No stack trace'));
                        return false;
                    }
                })()
                """
                
                print("🔧 Filter[\(filterID)] Executing script...")
                
                // Execute the script with timeout protection
                let startTime = Date()
                guard let result = jsContext.evaluateScript(script) else {
                    print("❌ Filter[\(filterID)] Script evaluation returned nil")
                    return false
                }
                let executionTime = Date().timeIntervalSince(startTime) * 1000 // Convert to ms
                
                print("⏱️ Filter[\(filterID)] Execution time: \(String(format: "%.2f", executionTime))ms")
                
                // Convert result to boolean
                let boolResult = result.toBool()
                print("✅ Filter[\(filterID)] END - Result: \(boolResult)")
                
                return boolResult
                
            } catch {
                print("❌ Filter[\(filterID)] Caught exception: \(error)")
                return false
            }
        }
    }
    
    // Create a ReplicationFilter from a JavaScript function string
  private static func createFilter(from functionString: String?) -> ReplicationFilter? {
    guard let functionString = functionString, !functionString.isEmpty else {
        return nil
    }
    
    return { (document, flags) -> Bool in
        // Add circuit breaker for repeated failures
        if let lastError = lastFilterError, filterCallCount > 10 {
            print("❌ Filter disabled due to repeated errors: \(lastError)")
            return true // Allow all documents through if filter is broken
        }
        
        do {
            // Wrap in exception handler
            var result = false
            var completed = false
            let semaphore = DispatchSemaphore(value: 0)
            
            jsContextQueue.async {
                do {
                    result = evaluateFilter(functionString, document: document, flags: flags)
                    completed = true
                } catch {
                    print("❌ Filter evaluation exception: \(error)")
                    result = false
                    completed = true
                }
                semaphore.signal()
            }
            
            // Wait with timeout
            let timeout = DispatchTime.now() + .milliseconds(10000)
            if semaphore.wait(timeout: timeout) == .timedOut {
                print("❌ Filter evaluation timed out for document: \(document.id)")
                // Don't return immediately - the filter might still complete
                // Wait a bit more to allow cleanup
                _ = semaphore.wait(timeout: .now() + .milliseconds(100))
                return false
            }
            
            // Only return result if actually completed
            return completed ? result : false
        } catch {
            print("❌ Unexpected error in filter: \(error)")
            return false
        }
    }
}

     public static func getFilterDebugInfo() -> String {
        return """
        Filter Debug Info:
        - Total filter calls: \(filterCallCount)
        - Last error: \(lastFilterError ?? "None")
        - JSContext valid: \(jsContext != nil)
        """
    }
    
    public static func replicatorConfigFromJson(_ data: [String: Any], collectionConfiguration: [CollectionConfigItem]) throws -> ReplicatorConfiguration {
       guard let target = data["target"] as? [String: Any],
              let url = target["url"] as? String,
              let replicatorType = data["replicatorType"] as? String,
              let continuous = data["continuous"] as? Bool,
              let acceptParentDomainCookies = data["acceptParentDomainCookies"] as? Bool,
              let acceptSelfSignedCerts = data["acceptSelfSignedCerts"] as? Bool,
              let allowReplicationInBackground = data["allowReplicationInBackground"] as? Bool,
              let autoPurgeEnabled = data["autoPurgeEnabled"] as? Bool,
              let heartbeat = data["heartbeat"] as? NSNumber,
              let maxAttempts = data["maxAttempts"] as? NSNumber,
              let maxAttemptWaitTime = data["maxAttemptWaitTime"] as? NSNumber
        else {
            throw ReplicatorError.fatalError(message: "Invalid JSON data")
        }
        
        let endpoint = URLEndpoint(url: URL(string: url)!)
        
        //set values from data
        var replConfig = ReplicatorConfiguration(target: endpoint)
        replConfig.acceptParentDomainCookie = acceptSelfSignedCerts
        replConfig.acceptParentDomainCookie = acceptParentDomainCookies
        replConfig.allowReplicatingInBackground = allowReplicationInBackground
        replConfig.continuous = continuous
        replConfig.enableAutoPurge = autoPurgeEnabled
    
        replConfig.heartbeat = TimeInterval(exactly: heartbeat.int64Value) ?? 300
        replConfig.maxAttemptWaitTime = TimeInterval(exactly: maxAttemptWaitTime.int64Value) ?? 0
        replConfig.maxAttempts = maxAttempts.uintValue
        
        //check for headers
        if let headers = data["headers"] as? [String: String] {
            replConfig.headers = headers
        }
        
        if let authenticatorData = data["authenticator"], !(authenticatorData is String && authenticatorData as! String == "")  {
            if let authenticatorConfig = authenticatorData as? [String: Any] {
                if let authenticator = ReplicatorHelper.replicatorAuthenticatorFromConfig(authenticatorConfig) {
                    replConfig.authenticator = authenticator
                }
            }
            
        }
        
       try  ReplicatorHelper.replicatorCollectionConfigFromJson(collectionConfiguration, replicationConfig:  &replConfig)
        
        switch replicatorType {
            case "PUSH_AND_PULL":
                replConfig.replicatorType = .pushAndPull
            case "PULL":
                replConfig.replicatorType = .pull
            case "PUSH":
                replConfig.replicatorType = .push
            default:
                throw ReplicatorError.fatalError(message: "Invalid replicatorType")
        }
        return replConfig
    }
    
     public static func replicatorCollectionConfigFromJson(_ data: [CollectionConfigItem], replicationConfig: inout ReplicatorConfiguration) throws {
        for item in data {
            var collections: [Collection] = []
            
            // Get all collections for this configuration
            for col in item.collections {
                guard let collection = try CollectionManager.shared.getCollection(
                    col.collection.name,
                    scopeName: col.collection.scopeName,
                    databaseName: col.collection.databaseName
                ) else {
                    throw CollectionError.unableToFindCollection(
                        collectionName: col.collection.name,
                        scopeName: col.collection.scopeName,
                        databaseName: col.collection.databaseName
                    )
                }
                collections.append(collection)
            }
            
            // Process the config part of the data
            var collectionConfig = CollectionConfiguration()
            
            // Set channels and document IDs
            if item.config.channels.count > 0 {
                collectionConfig.channels = item.config.channels
            }
            if item.config.documentIds.count > 0 {
                collectionConfig.documentIDs = item.config.documentIds
            }
            
            // Process push and pull filters
            if let pushFilterStr = item.config.pushFilter, !pushFilterStr.isEmpty {
                collectionConfig.pushFilter = createFilter(from: pushFilterStr)
            }
            
            if let pullFilterStr = item.config.pullFilter, !pullFilterStr.isEmpty {
                collectionConfig.pullFilter = createFilter(from: pullFilterStr)
            }
            
            // Add collections with their configuration
            replicationConfig.addCollections(collections, config: collectionConfig)
        }
    }
    
    private static func replicatorAuthenticatorFromConfig(_ config: [String: Any]?) -> Authenticator? {
        guard let type = config?["type"] as? String,
              let data = config?["data"] as? [String: Any] else {
            return nil
        }
        
        switch type {
            case "session":
                guard let sessionID = data["sessionID"] as? String,
                      let cookieName = data["cookieName"] as? String else {
                    return nil
                }
                return SessionAuthenticator(sessionID: sessionID, cookieName: cookieName)
                
            case "basic":
                guard let username = data["username"] as? String,
                      let password = data["password"] as? String else {
                    return nil
                }
                return BasicAuthenticator(username: username, password: password)
                
            default:
                return nil
        }
    }
    
    public static func generateReplicatorStatusJson(_ status: Replicator.Status) -> [String: Any] {
        var errorJson: [String: Any]?
        if let error = status.error {
            errorJson = [
                "message": error.localizedDescription
            ]
        }
        
        let progressJson: [String: Any] = [
            "completed": status.progress.completed,
            "total": status.progress.total
        ]
        
        if let errorJson = errorJson {
            return [
                "activityLevel": status.activity.rawValue,
                "error": errorJson,
                "progress": progressJson
            ]
        } else {
            return [
                "activityLevel": status.activity.rawValue,
                "progress": progressJson
            ]
        }
    }
    
    public static func generateReplicationJson(_ replication: [ReplicatedDocument], isPush: Bool) -> [String: Any] {
        var docs = [[String: Any]]()
        
        for document in replication {
            var flags = [String]()
            if document.flags.contains(.deleted) {
                flags.append("DELETED")
            }
            if document.flags.contains(.accessRemoved) {
                flags.append("ACCESS_REMOVED")
            }
            var documentDictionary: [String: Any] = ["id": document.id, "flags": flags, "scopeName": document.scope, "collectionName": document.collection]
            
            if let error = document.error {
                documentDictionary["error"] = [
                    "message": error.localizedDescription
                ]
            }
            
            docs.append(documentDictionary)
        }
        
        return [
            "isPush": isPush ? true : false,
            "documents": docs
        ]
    }
    
}
