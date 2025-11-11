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

    /// Creates a ReplicationFilter closure from a JavaScript function string
    ///
    /// **What it does:**
    /// - Converts a JavaScript function string (received from React Native) into a Swift closure
    /// - The closure evaluates the JS function against documents during replication
    /// - Used for push/pull filters to determine which documents to replicate
    ///
    /// **Parameters:**
    /// - `functionString`: JavaScript function as a string (optional)
    ///
    /// **Example parameter:**
    /// ```swift
    /// "(doc, flags) => { return doc.type === 'user' && !flags.includes('DELETED'); }"
    /// ```
    ///
    /// **Returns:**
    /// - `ReplicationFilter?`: Swift closure that takes (Document, DocumentFlags) -> Bool
    /// - `nil` if functionString is nil or empty
    ///
    /// **Example return value:**
    /// ```swift
    /// // Returns a closure like:
    /// { (document, flags) -> Bool in
    ///     // Evaluates the JS function against the document
    ///     return true  // or false based on JS evaluation
    /// }
    /// ```
    ///
    /// **Usage:**
    /// ```swift
    /// let pushFilter = createFilter(from: "(doc, flags) => doc.type === 'user'")
    /// collectionConfig.pushFilter = pushFilter
    /// ```
    private static func createFilter(from functionString: String?) -> ReplicationFilter? {
        guard let functionString = functionString, !functionString.isEmpty else {
            return nil
        }
        
        return { (document, flags) -> Bool in
            return evaluateFilter(functionString, document: document, flags: flags)
        }
    }
    
    /// **[NEW API]** Creates a ReplicatorConfiguration from JSON data using NEW API pattern
    ///
    /// **What it does:**
    /// - Main entry point for creating a replicator configuration from React Native
    /// - Uses NEW API pattern with CollectionConfigurationDto
    /// - Builds CollectionConfiguration array FIRST, then creates ReplicatorConfiguration
    /// - Uses ReplicatorConfiguration(collections:target:) constructor
    ///
    /// **Parameters:**
    /// - `data`: Dictionary containing replicator settings from JavaScript
    /// - `collectionConfiguration`: Array of CollectionConfigurationDto (NEW API)
    ///
    /// **Example data parameter (JSON from JavaScript):**
    /// ```json
    /// {
    ///   "target": {"url": "ws://localhost:4984/mydb"},
    ///   "replicatorType": "PUSH_AND_PULL",
    ///   "continuous": true,
    ///   "acceptParentDomainCookies": false,
    ///   "acceptSelfSignedCerts": true,
    ///   "allowReplicationInBackground": false,
    ///   "autoPurgeEnabled": true,
    ///   "heartbeat": 300,
    ///   "maxAttempts": 10,
    ///   "maxAttemptWaitTime": 300,
    ///   "pinnedServerCertificate": "",
    ///   "headers": {"Authorization": "Bearer token123"},
    ///   "authenticator": {
    ///     "type": "basic",
    ///     "data": {"username": "user", "password": "pass"}
    ///   },
    ///   "collectionConfig": "[{\"collection\":{...},\"config\":{...}}]"
    /// }
    /// ```
    ///
    /// **Example collectionConfiguration parameter:**
    /// ```swift
    /// [
    ///   CollectionConfigurationDto(
    ///     collection: CollectionDto(name: "users", scopeName: "_default", databaseName: "mydb"),
    ///     config: ConfigDto(channels: ["public"], documentIds: [], pushFilter: nil, pullFilter: nil)
    ///   ),
    ///   CollectionConfigurationDto(
    ///     collection: CollectionDto(name: "orders", scopeName: "_default", databaseName: "mydb"),
    ///     config: ConfigDto(channels: ["orders", "admin"], documentIds: ["order-1"], pushFilter: nil, pullFilter: nil)
    ///   )
    /// ]
    /// ```
    ///
    /// **Returns:**
    /// - `ReplicatorConfiguration`: Fully configured iOS native replicator configuration
    ///
    /// **Example return value:**
    /// ```swift
    /// ReplicatorConfiguration(
    ///   collections: [
    ///     CollectionConfiguration(collection: Collection(name: "users"), channels: ["public"]),
    ///     CollectionConfiguration(collection: Collection(name: "orders"), channels: ["orders", "admin"])
    ///   ],
    ///   target: URLEndpoint(url: "ws://localhost:4984/mydb"),
    ///   continuous: true,
    ///   replicatorType: .pushAndPull,
    ///   authenticator: BasicAuthenticator(username: "user", password: "pass"),
    ///   heartbeat: 300,
    ///   maxAttempts: 10,
    ///   maxAttemptWaitTime: 300,
    ///   enableAutoPurge: true,
    ///   acceptParentDomainCookie: false,
    ///   acceptOnlySelfSignedServerCertificate: true,
    ///   allowReplicatingInBackground: false
    /// )
    /// ```
    ///
    /// **Throws:**
    /// - `ReplicatorError.fatalError`: If required fields are missing or invalid
    /// - `CollectionError`: If collection lookup fails
    ///
    /// **NEW API Pattern:**
    /// 1. Parse collection configuration DTOs from JSON
    /// 2. Build CollectionConfiguration array using buildReplicatorCollectionConfigurationsFromJson()
    /// 3. Create ReplicatorConfiguration with collections AND endpoint together
    /// 4. Set all other properties after construction
    
    /// **[DUAL API SUPPORT]** Creates ReplicatorConfiguration from JSON string (auto-detects format)
    ///
    /// **What it does:**
    /// - Receives JSON string containing collection configuration
    /// - Automatically detects NEW API or OLD API format
    /// - Parses JSON and routes to appropriate processing
    /// - Returns fully configured ReplicatorConfiguration
    ///
    /// **Parameters:**
    /// - `data`: Dictionary containing all replicator settings
    /// - `collectionConfigJson`: JSON string in NEW or OLD format
    ///
    /// **Throws:**
    /// - `ReplicatorError.fatalError`: If JSON parsing fails or format is unrecognized
    /// - `CollectionError`: If collection lookup fails
    public static func replicatorConfigFromJson(
        _ data: [String: Any],
        collectionConfigJson: String
    ) throws -> ReplicatorConfiguration {
        print("\n╔════════════════════════════════════════════════════════════════╗")
        print("║  [ReplicatorHelper] STARTING REPLICATOR CREATION               ║")
        print("╚════════════════════════════════════════════════════════════════╝")
        
        // Parse JSON string to data
        print("[Step 1] Converting JSON string to data...")
        guard let jsonData = collectionConfigJson.data(using: .utf8) else {
            print("[Step 1] ❌ FAILED: Unable to convert JSON string to data")
            throw ReplicatorError.fatalError(message: "Unable to convert JSON string to data")
        }
        print("[Step 1] ✅ JSON string converted to data")
        
        // Try to parse as generic JSON to detect format
        print("[Step 2] Parsing JSON to detect format...")
        guard let jsonArray = try? JSONSerialization.jsonObject(with: jsonData) as? [[String: Any]],
              let firstItem = jsonArray.first else {
            print("[Step 2] ❌ FAILED: Invalid JSON format")
            throw ReplicatorError.fatalError(message: "Invalid JSON format: expected array of objects")
        }
        print("[Step 2] ✅ JSON array parsed, count:", jsonArray.count)
        
        // Detect format by checking for "collection" (NEW) vs "collections" (OLD)
        print("[Step 3] Detecting API format...")
        let isNewApi = firstItem["collection"] != nil
        let isOldApi = firstItem["collections"] != nil
        print("[Step 3] Detection result - isNewApi:", isNewApi, "isOldApi:", isOldApi)
        print("[Step 3] firstItem keys:", Array(firstItem.keys))
        
        if isNewApi {
            print("[Step 4] 🔷 Using NEW API path")
            let decoder = JSONDecoder()
            let collectionConfig = try decoder.decode([CollectionConfigurationDto].self, from: jsonData)
            print("[Step 4] ✅ Decoded NEW API config, count:", collectionConfig.count)
            return try replicatorConfigFromJson(data, collectionConfiguration: collectionConfig)
        } else if isOldApi {
            print("[Step 4] 🔶 Using OLD API path")
            let decoder = JSONDecoder()
            let collectionConfig = try decoder.decode([CollectionConfigItem].self, from: jsonData)
            print("[Step 4] ✅ Decoded OLD API config, count:", collectionConfig.count)
            return try replicatorConfigFromJsonOldApi(data, collectionConfiguration: collectionConfig)
        } else {
            print("[Step 4] ❌ FAILED: Unrecognized format (no 'collection' or 'collections' key)")
            throw ReplicatorError.fatalError(message: "Unrecognized collection configuration format")
        }
    }
    
    /// **[NEW API]** Creates ReplicatorConfiguration with typed DTOs
    ///
    /// **Internal function** - Called after JSON parsing and format detection
    public static func replicatorConfigFromJson(
        _ data: [String: Any],
        collectionConfiguration: [CollectionConfigurationDto]
    ) throws -> ReplicatorConfiguration {
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 1: Parse and validate required fields
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
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
              let maxAttemptWaitTime = data["maxAttemptWaitTime"] as? NSNumber,
              let pinnedServerCertificate = data["pinnedServerCertificate"] as? String
        else {
            throw ReplicatorError.fatalError(message: "Invalid JSON data: required fields missing")
        }
        
        // Validate collections array is not empty
        guard !collectionConfiguration.isEmpty else {
            throw ReplicatorError.fatalError(message: "At least one collection configuration is required")
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 2: Create endpoint
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        guard let endpointUrl = URL(string: url) else {
            throw ReplicatorError.fatalError(message: "Invalid target URL: \(url)")
        }
        let endpoint = URLEndpoint(url: endpointUrl)
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 3: Build CollectionConfiguration array (NEW API)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        let collectionConfigs = try buildReplicatorCollectionConfigurationsFromJson(collectionConfiguration)
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 4: Create ReplicatorConfiguration with collections AND endpoint (NEW API)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        var replConfig = ReplicatorConfiguration(collections: collectionConfigs, target: endpoint)
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 5: Set replicator type
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        switch replicatorType {
        case "PUSH_AND_PULL":
            replConfig.replicatorType = .pushAndPull
        case "PULL":
            replConfig.replicatorType = .pull
        case "PUSH":
            replConfig.replicatorType = .push
        default:
            throw ReplicatorError.fatalError(message: "Invalid replicatorType: \(replicatorType)")
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 6: Set boolean properties
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        replConfig.continuous = continuous
        replConfig.acceptParentDomainCookie = acceptParentDomainCookies
        replConfig.acceptOnlySelfSignedServerCertificate = acceptSelfSignedCerts
        replConfig.allowReplicatingInBackground = allowReplicationInBackground
        replConfig.enableAutoPurge = autoPurgeEnabled
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 7: Set numeric properties
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        replConfig.heartbeat = TimeInterval(exactly: heartbeat.int64Value) ?? 300
        replConfig.maxAttempts = maxAttempts.uintValue
        replConfig.maxAttemptWaitTime = TimeInterval(exactly: maxAttemptWaitTime.int64Value) ?? 0
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 8: Set pinned server certificate (if provided)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if !pinnedServerCertificate.isEmpty {
            guard let certData = Data(base64Encoded: pinnedServerCertificate) else {
                throw ReplicatorError.fatalError(message: "Invalid pinned server certificate: not valid base64")
            }
            guard let pinnedCert = SecCertificateCreateWithData(nil, certData as CFData) else {
                throw ReplicatorError.fatalError(message: "Invalid pinned server certificate: not valid certificate")
            }
            replConfig.pinnedServerCertificate = pinnedCert
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 9: Set headers (if provided)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if let headers = data["headers"] as? [String: String], !headers.isEmpty {
            replConfig.headers = headers
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 10: Set authenticator (if provided)
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        if let authenticatorData = data["authenticator"],
           !(authenticatorData is String && (authenticatorData as! String).isEmpty) {
            if let authenticatorConfig = authenticatorData as? [String: Any] {
                if let authenticator = ReplicatorHelper.replicatorAuthenticatorFromConfig(authenticatorConfig) {
                    replConfig.authenticator = authenticator
                }
            }
        }
        
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        // STEP 11: Return fully configured ReplicatorConfiguration
        // ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
        
        return replConfig
    }
    
    /// **[OLD API SUPPORT]** Creates ReplicatorConfiguration from OLD API format
    ///
    /// **What it does:**
    /// - Processes OLD API format where multiple collections can share one config
    /// - Creates ReplicatorConfiguration using endpoint-only constructor
    /// - Adds collections with their shared config via mutation
    /// - Maintains backward compatibility with existing implementations
    ///
    /// **Parameters:**
    /// - `data`: Dictionary containing all replicator settings
    /// - `collectionConfiguration`: Array of CollectionConfigItem (OLD API format)
    ///
    /// **OLD API Format:**
    /// ```swift
    /// CollectionConfigItem(
    ///   collections: [CollectionDtoWrapper(collection: CollectionDto(...))],
    ///   config: ConfigDto(channels: [...], ...)
    /// )
    /// ```
    ///
    /// **Throws:**
    /// - `ReplicatorError.fatalError`: If required fields are missing or invalid
    /// - `CollectionError`: If collection lookup fails
    private static func replicatorConfigFromJsonOldApi(
        _ data: [String: Any],
        collectionConfiguration: [CollectionConfigItem]
    ) throws -> ReplicatorConfiguration {
        print("\n[OLD API] ═══════════════════════════════════════════════════════")
        print("[OLD API] Starting OLD API ReplicatorConfiguration creation")
        print("[OLD API] ═══════════════════════════════════════════════════════")
        
        // Parse and validate required fields
        print("[OLD API Step 1] Parsing required fields from data dictionary...")
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
              let maxAttemptWaitTime = data["maxAttemptWaitTime"] as? NSNumber,
              let pinnedServerCertificate = data["pinnedServerCertificate"] as? String
        else {
            print("[OLD API Step 1] ❌ FAILED: Required fields missing")
            throw ReplicatorError.fatalError(message: "Invalid JSON data: required fields missing")
        }
        print("[OLD API Step 1] ✅ All required fields parsed")
        print("[OLD API Step 1] - URL:", url)
        print("[OLD API Step 1] - Type:", replicatorType)
        print("[OLD API Step 1] - Continuous:", continuous)
        
        // Create endpoint
        print("[OLD API Step 2] Creating URLEndpoint...")
        guard let endpointUrl = URL(string: url) else {
            print("[OLD API Step 2] ❌ FAILED: Invalid URL")
            throw ReplicatorError.fatalError(message: "Invalid target URL: \(url)")
        }
        let endpoint = URLEndpoint(url: endpointUrl)
        print("[OLD API Step 2] ✅ URLEndpoint created")
        
        // OLD API: Create config with endpoint only
        print("[OLD API Step 3] Creating ReplicatorConfiguration with endpoint...")
        var replConfig = ReplicatorConfiguration(target: endpoint)
        print("[OLD API Step 3] ✅ ReplicatorConfiguration created")
        
        // Set replicator type
        print("[OLD API Step 4] Setting replicator type: \(replicatorType)")
        switch replicatorType {
        case "PUSH_AND_PULL":
            replConfig.replicatorType = .pushAndPull
        case "PULL":
            replConfig.replicatorType = .pull
        case "PUSH":
            replConfig.replicatorType = .push
        default:
            print("[OLD API Step 4] ❌ FAILED: Unknown replicator type")
            throw ReplicatorError.fatalError(message: "Unknown replicator type: \(replicatorType)")
        }
        print("[OLD API Step 4] ✅ Replicator type set")
        
        // Set other properties
        print("[OLD API Step 5] Setting configuration properties...")
        replConfig.continuous = continuous
        replConfig.acceptParentDomainCookie = acceptParentDomainCookies
        replConfig.acceptOnlySelfSignedServerCertificate = acceptSelfSignedCerts
        replConfig.allowReplicatingInBackground = allowReplicationInBackground
        replConfig.enableAutoPurge = autoPurgeEnabled
        replConfig.heartbeat = TimeInterval(heartbeat.intValue)
        replConfig.maxAttempts = UInt(maxAttempts.intValue)
        replConfig.maxAttemptWaitTime = TimeInterval(maxAttemptWaitTime.intValue)
        print("[OLD API Step 5] ✅ All properties set")
        
        // Handle pinned certificate
        print("[OLD API Step 6] Processing pinned certificate...")
        if !pinnedServerCertificate.isEmpty {
            if let pinnedCert = pinnedServerCertificate.data(using: .utf8) {
                replConfig.pinnedServerCertificate = SecCertificateCreateWithData(nil, pinnedCert as CFData)
                print("[OLD API Step 6] ✅ Pinned certificate set")
            } else {
                print("[OLD API Step 6] ⚠️ Pinned certificate data conversion failed")
            }
        } else {
            print("[OLD API Step 6] ℹ️ No pinned certificate")
        }
        
        // Process authenticator
        print("[OLD API Step 7] Processing authenticator...")
        if let authenticator = data["authenticator"] as? [String: Any],
           let type = authenticator["type"] as? String,
           let authData = authenticator["data"] as? [String: Any] {
            print("[OLD API Step 7] Authenticator type:", type)
            switch type {
            case "basic":
                if let username = authData["username"] as? String,
                   let password = authData["password"] as? String {
                    replConfig.authenticator = BasicAuthenticator(username: username, password: password)
                    print("[OLD API Step 7] ✅ Basic authenticator set")
                }
            case "session":
                if let sessionID = authData["sessionId"] as? String {
                    let cookieName = authData["cookieName"] as? String ?? "SyncGatewaySession"
                    replConfig.authenticator = SessionAuthenticator(sessionID: sessionID, cookieName: cookieName)
                    print("[OLD API Step 7] ✅ Session authenticator set")
                }
            default:
                print("[OLD API Step 7] ⚠️ Unknown authenticator type")
                break
            }
        } else {
            print("[OLD API Step 7] ℹ️ No authenticator")
        }
        
        // Process headers
        print("[OLD API Step 8] Processing headers...")
        if let headers = data["headers"] as? [String: String] {
            replConfig.headers = headers
            print("[OLD API Step 8] ✅ Headers set:", headers.count, "entries")
        } else {
            print("[OLD API Step 8] ℹ️ No headers")
        }
        
        // OLD API: Add collections via mutation
        print("\n[OLD API Step 9] Processing \(collectionConfiguration.count) collection config item(s)...")
        for (index, item) in collectionConfiguration.enumerated() {
            print("\n[OLD API Step 9.\(index + 1)] Processing config item \(index + 1)/\(collectionConfiguration.count)")
            print("[OLD API Step 9.\(index + 1)] Collections in this item: \(item.collections.count)")
            var collections: [Collection] = []
            
            for (collIndex, wrapper) in item.collections.enumerated() {
                print("\n[OLD API Step 9.\(index + 1).\(collIndex + 1)] Processing collection \(collIndex + 1)/\(item.collections.count)")
                print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] Database: '\(wrapper.collection.databaseName)'")
                print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] Collection: '\(wrapper.collection.name)'")
                print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] Scope: '\(wrapper.collection.scopeName)'")
                
                print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] Looking up database...")
                guard let db = DatabaseManager.shared.getDatabase(wrapper.collection.databaseName) else {
                    print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] ❌ FAILED: Database not found")
                    throw CollectionError.databaseNotOpen(name: wrapper.collection.databaseName)
                }
                print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] ✅ Database found")
                
                print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] Looking up collection...")
                guard let collection = try db.collection(name: wrapper.collection.name, scope: wrapper.collection.scopeName) else {
                    print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] ❌ FAILED: Collection not found")
                    throw CollectionError.unableToFindCollection(
                        collectionName: wrapper.collection.name,
                        scopeName: wrapper.collection.scopeName,
                        databaseName: wrapper.collection.databaseName
                    )
                }
                print("[OLD API Step 9.\(index + 1).\(collIndex + 1)] ✅ Collection found")
                collections.append(collection)
            }
            
            // Build collection config from shared ConfigDto
            // Note: Using deprecated API for OLD API backward compatibility
            print("\n[OLD API Step 9.\(index + 1).Config] Building CollectionConfiguration...")
            var config = CollectionConfiguration()
            
            // Set channels
            if !item.config.channels.isEmpty {
                print("[OLD API Step 9.\(index + 1).Config] Setting channels: \(item.config.channels)")
                config.channels = item.config.channels
            } else {
                print("[OLD API Step 9.\(index + 1).Config] No channels")
            }
            
            // Set document IDs
            if !item.config.documentIds.isEmpty {
                print("[OLD API Step 9.\(index + 1).Config] Setting documentIDs: \(item.config.documentIds)")
                config.documentIDs = item.config.documentIds
            } else {
                print("[OLD API Step 9.\(index + 1).Config] No document IDs")
            }
            
            // Set push filter
            if let pushFilter = item.config.pushFilter, !pushFilter.isEmpty {
                print("[OLD API Step 9.\(index + 1).Config] Setting push filter")
                config.pushFilter = createFilter(from: pushFilter)
            } else {
                print("[OLD API Step 9.\(index + 1).Config] No push filter")
            }
            
            // Set pull filter
            if let pullFilter = item.config.pullFilter, !pullFilter.isEmpty {
                print("[OLD API Step 9.\(index + 1).Config] Setting pull filter")
                config.pullFilter = createFilter(from: pullFilter)
            } else {
                print("[OLD API Step 9.\(index + 1).Config] No pull filter")
            }
            print("[OLD API Step 9.\(index + 1).Config] ✅ CollectionConfiguration built")
            
            // Add collections with shared config
            // Note: Using deprecated API for OLD API backward compatibility
            print("\n[OLD API Step 9.\(index + 1).Add] Adding \(collections.count) collection(s) to replicator config...")
            do {
                replConfig.addCollections(collections, config: config)
                print("[OLD API Step 9.\(index + 1).Add] ✅ Successfully added collections")
            } catch {
                print("[OLD API Step 9.\(index + 1).Add] ❌ FAILED: \(error)")
                throw error
            }
        }
        
        print("\n[OLD API Step 10] ✅ All collection configurations processed")
        print("\n╔════════════════════════════════════════════════════════════════╗")
        print("║  [OLD API] ✅ REPLICATOR CREATION SUCCESSFUL                   ║")
        print("╚════════════════════════════════════════════════════════════════╝\n")
        return replConfig
    }


    /// **[NEW API]** Builds an array of CollectionConfiguration objects from DTOs
///
/// **What it does:**
/// - Converts NEW API DTOs into iOS native CollectionConfiguration objects
/// - Each CollectionConfiguration is paired with its specific collection
/// - Allows different collections to have different configurations
/// - Uses the NEW iOS native pattern: CollectionConfiguration(collection:)
///
/// **Parameters:**
/// - `data`: Array of CollectionConfigurationDto from JavaScript
///
/// **Example parameter:**
/// ```swift
/// [
///   CollectionConfigurationDto(
///     collection: CollectionDto(name: "users", scopeName: "_default", databaseName: "mydb"),
///     config: ConfigDto(channels: ["public"], documentIds: [], pushFilter: nil, pullFilter: nil)
///   ),
///   CollectionConfigurationDto(
///     collection: CollectionDto(name: "orders", scopeName: "_default", databaseName: "mydb"),
///     config: ConfigDto(channels: ["orders"], documentIds: ["order-1"], pushFilter: nil, pullFilter: nil)
///   )                                 
/// ]
/// ```
///
/// **Returns:**
/// - `[CollectionConfiguration]`: Array of iOS native CollectionConfiguration objects
///
/// **Example return value:**
/// ```swift
/// [
///   CollectionConfiguration(
///     collection: Collection(name: "users"),
///     channels: ["public"],
///     documentIDs: []
///   ),
///   CollectionConfiguration(
///     collection: Collection(name: "orders"),
///     channels: ["orders"],
///     documentIDs: ["order-1"]
///   )
/// ]
/// ```
///
/// **Throws:**
/// - `CollectionError.unableToFindCollection`: If any collection doesn't exist
///
/// **Usage:**
/// ```swift
/// let collectionConfigs = try buildCollectionConfigurations(dtoArray)
/// let replConfig = ReplicatorConfiguration(collections: collectionConfigs, target: endpoint)
/// ```
    public static func buildReplicatorCollectionConfigurationsFromJson(_ data: [CollectionConfigurationDto]) throws -> [CollectionConfiguration]{
        var collectionConfigs: [CollectionConfiguration] = []

        for item in data {
            guard let collection = try CollectionManager.shared.getCollection(
                item.collection.name,
                scopeName: item.collection.scopeName,
                databaseName: item.collection.databaseName
            )
            else{
                throw CollectionError.unableToFindCollection(
                    collectionName: item.collection.name,
                    scopeName: item.collection.scopeName,
                    databaseName: item.collection.databaseName
                )
            }

        // 2. Create CollectionConfiguration WITH the collection (NEW API)
        var colConfig = CollectionConfiguration(collection: collection)

        // 3. Set configuration properties if config is provided
        if let config = item.config { 

            // Set channels (for pull replication)
            if config.channels.count > 0 {
                colConfig.channels = config.channels
            }

            // Set document IDs filter
            if config.documentIds.count > 0 {
                colConfig.documentIDs = config.documentIds
            }

            // Set push filter
            if let pushFilterStr = config.pushFilter, !pushFilterStr.isEmpty {
                colConfig.pushFilter = createFilter(from: pushFilterStr)
            }
            
            // Set pull filter
            if let pullFilterStr = config.pullFilter, !pullFilterStr.isEmpty {
                colConfig.pullFilter = createFilter(from: pullFilterStr)
            }

        }

        collectionConfigs.append(colConfig)


        }

        return collectionConfigs

    }

    
    /// Creates an Authenticator from configuration dictionary
    ///
    /// **What it does:**
    /// - Converts authentication configuration from JavaScript into iOS native Authenticator
    /// - Supports BasicAuthenticator and SessionAuthenticator
    ///
    /// **Parameters:**
    /// - `config`: Dictionary containing authenticator type and credentials
    ///
    /// **Example parameter (Basic Auth):**
    /// ```swift
    /// [
    ///   "type": "basic",
    ///   "data": [
    ///     "username": "admin",
    ///     "password": "password123"
    ///   ]
    /// ]
    /// ```
    ///
    /// **Example parameter (Session Auth):**
    /// ```swift
    /// [
    ///   "type": "session",
    ///   "data": [
    ///     "sessionID": "904ac010c2f4001a9c2ee8f9",
    ///     "cookieName": "SyncGatewaySession"
    ///   ]
    /// ]
    /// ```
    ///
    /// **Returns:**
    /// - `Authenticator?`: iOS native authenticator instance
    /// - `nil` if config is nil, empty, or invalid
    ///
    /// **Example return values:**
    /// ```swift
    /// BasicAuthenticator(username: "admin", password: "password123")
    /// // or
    /// SessionAuthenticator(sessionID: "904ac010c2f4001a9c2ee8f9", cookieName: "SyncGatewaySession")
    /// // or
    /// nil  // if invalid/missing
    /// ```
    ///
    /// **Supported Types:**
    /// - "basic": Username/password authentication
    /// - "session": Sync Gateway session authentication
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
    
    /// Converts iOS native Replicator.Status to JSON dictionary for JavaScript
    ///
    /// **What it does:**
    /// - Converts iOS native replicator status into a format that can be sent to React Native
    /// - Includes activity level, progress, and error information
    ///
    /// **Parameters:**
    /// - `status`: iOS native Replicator.Status object
    ///
    /// **Example parameter:**
    /// ```swift
    /// Replicator.Status(
    ///   activity: .busy,
    ///   progress: Replicator.Progress(completed: 50, total: 100),
    ///   error: nil
    /// )
    /// ```
    ///
    /// **Returns:**
    /// - `[String: Any]`: Dictionary suitable for JSON serialization to JavaScript
    ///
    /// **Example return value (without error):**
    /// ```swift
    /// [
    ///   "activityLevel": 2,  // .busy
    ///   "progress": [
    ///     "completed": 50,
    ///     "total": 100
    ///   ]
    /// ]
    /// ```
    ///
    /// **Example return value (with error):**
    /// ```swift
    /// [
    ///   "activityLevel": 3,  // .stopped
    ///   "progress": [
    ///     "completed": 25,
    ///     "total": 100
    ///   ],
    ///   "error": [
    ///     "message": "Connection refused"
    ///   ]
    /// ]
    /// ```
    ///
    /// **Activity Levels:**
    /// - 0: .stopped
    /// - 1: .offline
    /// - 2: .connecting
    /// - 3: .idle
    /// - 4: .busy
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
    
    /// Converts replicated documents to JSON dictionary for JavaScript
    ///
    /// **What it does:**
    /// - Converts an array of ReplicatedDocument objects to a format suitable for React Native
    /// - Used for document replication change listeners
    /// - Includes document IDs, flags, collection info, and errors
    ///
    /// **Parameters:**
    /// - `replication`: Array of ReplicatedDocument objects from iOS native replicator
    /// - `isPush`: Whether this is a push or pull replication event
    ///
    /// **Example parameters:**
    /// ```swift
    /// replication: [
    ///   ReplicatedDocument(
    ///     id: "user::123",
    ///     flags: [.deleted],
    ///     scope: "_default",
    ///     collection: "users",
    ///     error: nil
    ///   ),
    ///   ReplicatedDocument(
    ///     id: "order::456",
    ///     flags: [],
    ///     scope: "_default",
    ///     collection: "orders",
    ///     error: Error(message: "Conflict")
    ///   )
    /// ]
    /// isPush: true
    /// ```
    ///
    /// **Returns:**
    /// - `[String: Any]`: Dictionary containing isPush flag and documents array
    ///
    /// **Example return value:**
    /// ```swift
    /// [
    ///   "isPush": true,
    ///   "documents": [
    ///     [
    ///       "id": "user::123",
    ///       "flags": ["DELETED"],
    ///       "scopeName": "_default",
    ///       "collectionName": "users"
    ///     ],
    ///     [
    ///       "id": "order::456",
    ///       "flags": [],
    ///       "scopeName": "_default",
    ///       "collectionName": "orders",
    ///       "error": [
    ///         "message": "Conflict"
    ///       ]
    ///     ]
    ///   ]
    /// ]
    /// ```
    ///
    /// **Document Flags:**
    /// - "DELETED": Document was deleted
    /// - "ACCESS_REMOVED": User lost access to document
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
