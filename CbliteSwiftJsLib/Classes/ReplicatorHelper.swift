//
//  ReplicatorHelper.swift
//  CbliteSwiftJsLib
//
//  Created by Aaron LaBeau on 07/04/24.
//

import Foundation
import CouchbaseLiteSwift

public struct ReplicatorHelper {

    public static func replicatorConfigFromJson(_ data: [String: Any]) throws -> ReplicatorConfiguration {
        guard let authenticatorData = data["authenticator"] as? [String: Any],
              let target = data["target"] as? [String: Any],
              let url = target["url"] as? String,
              let replicatorType = data["replicatorType"] as? String,
              let continuous = data["continuous"] as? Bool,
              let collectionConfig = data["collectionConfig"] as? [String: Any] else {
            throw ReplicatorError.fatalError(message: "Invalid JSON data")
        }

        let endpoint = URLEndpoint(url: URL(string: url)!)
        var replConfig = ReplicatorConfiguration(target: endpoint)

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

        replConfig.continuous = continuous

        if let authenticator = ReplicatorHelper.replicatorAuthenticatorFromConfig(authenticatorData) {
            replConfig.authenticator = authenticator
        }

        return replConfig
    }

    private static func replicatorCollectionConfigFromJson(_ data: [String: Any]) throws -> (Set<Collection>, CollectionConfiguration) {
        
        //work on the collections sent in as part of the configuration with an array of collectionName, scopeName, and databaseName
        guard let collectionData = data["collections"] as? [[String: String]] else {
            throw ReplicatorError.configurationError(message: "collections doesn't include collections in the proper format")
        }
        guard let config = data["config"] as? [String: Any] else {
            throw ReplicatorError.configurationError(message: "ReplicationConfig collection config is incorrect format")
        }
        
        var collections: Set<Collection> = []
    
        for collectionItem in collectionData {
            guard let collectionName = collectionItem["collectionName"],
                  let scopeName = collectionItem["scopeName"],
                  let databaseName = collectionItem["databaseName"] else {
                // Handle the case where any required key is missing
                throw ReplicatorError.configurationError(message: "Error: collections missing required key in collection data - collectionName, scopeName, or databaseName")
            }
            guard let collection = try CollectionManager.shared.getCollection(collectionName, scopeName: scopeName, databaseName: databaseName) else {
                throw CollectionError.unableToFindCollection(collectionName: collectionName, scopeName: scopeName, databaseName: databaseName)
            }
            collections.insert(collection)
        }
        //process the config part of the data
        var collectionConfig = CollectionConfiguration()
        
        //get the channels and documentIds to filter for the collections
        //these are optional
        if let channels = config["channels"] as? [String] {
            collectionConfig.channels = channels
        }
        if let documentIds = config["documentIds"] as? [String] {
            collectionConfig.documentIDs = documentIds
        }
        return (collections, collectionConfig)
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

    public static func generateReplicationJson(_ replication: DocumentReplication) -> [String: Any] {
        var docs = [[String: Any]]()

        for document in replication.documents {
            var flags = [String]()
            if document.flags.contains(.deleted) {
                flags.append("DELETED")
            }
            if document.flags.contains(.accessRemoved) {
                flags.append("ACCESS_REMOVED")
            }
            var documentDictionary: [String: Any] = ["id": document.id, "flags": flags]

            if let error = document.error {
                documentDictionary["error"] = [
                    "message": error.localizedDescription
                ]
            }

            docs.append(documentDictionary)
        }

        return [
            "direction": replication.isPush ? "PUSH" : "PULL",
            "documents": docs
        ]
    }

}
