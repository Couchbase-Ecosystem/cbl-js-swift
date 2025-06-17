//
//  ReplicatorHelper.swift
//  CbliteSwiftJsLib
//

import Foundation
import JavaScriptCore
import CouchbaseLiteSwift

public struct ReplicatorHelper {
    private static let jsContextQueue = DispatchQueue(label: "com.couchbase.jscontext")
    
    private static let jsContext: JSContext = {
        let context = JSContext()!
        
        // Setup console.log for debugging
        let logFunction: @convention(block) (String) -> Void = { message in
            print("JSFilter: \(message)")
        }
        
        context.setObject(logFunction, forKeyedSubscript: "log" as NSString)
        context.evaluateScript("var console = { log: log };")
        
        // Handle JavaScript exceptions
        context.exceptionHandler = { context, exception in
            if let exc = exception {
                print("JSFilter Exception: \(exc.toString() ?? "unknown error")")
            }
        }
        
        return context
    }()
    
    private static func evaluateFilter(_ filterFunction: String, document: Document, flags: DocumentFlags) -> Bool {
        return jsContextQueue.sync {
                autoreleasepool {
                // Convert document to dictionary
                var docDict = document.toDictionary()
                docDict["id"] = document.id

                // Create flags array
                let flagsArray: [String] = [
                    flags.contains(.deleted) ? "DELETED" : nil,
                    flags.contains(.accessRemoved) ? "ACCESS_REMOVED" : nil
                ].compactMap { $0 }

                // Set objects directly in JSContext
                jsContext.setObject(docDict, forKeyedSubscript: "currentDocument" as NSString)
                jsContext.setObject(flagsArray, forKeyedSubscript: "currentFlags" as NSString)
                jsContext.setObject(filterFunction, forKeyedSubscript: "filterFunctionString" as NSString)
                
                // Create and execute the filter script
                let script = """
                (function() {
                    try {
                        // Enum for flags
                        const ReplicatedDocumentFlag = {
                            DELETED: 'DELETED',
                            ACCESS_REMOVED: 'ACCESS_REMOVED'
                        };
                        // Create the filter function from string
                        const filterFunc = eval('(' + filterFunctionString + ')');

                        const result = filterFunc(currentDocument, currentFlags);
                        // Ensure we return a boolean
                        return !!result;
                    } catch (e) {
                        console.log('Filter error: ' + e.toString());
                        console.log('Stack: ' + (e.stack || 'No stack trace'));
                        return false;
                    }
                })()
                """
                
                // Execute the script
                guard let result = jsContext.evaluateScript(script) else {
                    print("JSFilter: Script evaluation returned nil")
                    return false
                }

                // Clear references
                jsContext.setObject(nil, forKeyedSubscript: "currentDocument" as NSString)
                jsContext.setObject(nil, forKeyedSubscript: "currentFlags" as NSString)
                jsContext.setObject(nil, forKeyedSubscript: "filterFunctionString" as NSString)
                
                // Convert result to boolean
                return result.toBool()
            }
        }
    }

     // Create a ReplicationFilter from a JavaScript function string
    private static func createFilter(from functionString: String?) -> ReplicationFilter? {
        guard let functionString = functionString, !functionString.isEmpty else {
            return nil
        }
        
        return { (document, flags) -> Bool in
            return evaluateFilter(functionString, document: document, flags: flags)
        }
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
    
    public static func replicatorCollectionConfigFromJson(_ data:  [CollectionConfigItem], replicationConfig: inout ReplicatorConfiguration) throws {
        
        //work on the collections sent in as part of the configuration with an array of collectionName, scopeName, and databaseName
        for item in data {
            
            var collections: [Collection] = []
            
            for col in item.collections {
                
                guard let collection = try CollectionManager.shared.getCollection(col.collection.name, scopeName: col.collection.scopeName, databaseName: col.collection.databaseName) else {
                    throw CollectionError.unableToFindCollection(collectionName: col.collection.name, scopeName: col.collection.scopeName, databaseName: col.collection.databaseName)
                }
                collections.append(collection)
            }
            
            //process the config part of the data
            var collectionConfig = CollectionConfiguration()
            
            //get the channels and documentIds to filter for the collections
            //these are optional
            if item.config.channels.count > 0 {
                collectionConfig.channels =  item.config.channels
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
