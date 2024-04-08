//
//  ReplicatorHelper.swift
//  CbliteSwiftJsLib
//
//  Created by Aaron LaBeau on 07/04/24.
//

import Foundation
import CouchbaseLiteSwift

public struct ReplicatorHelper {

    public static func replicatorConfigFromJson(_ database: Database, data: [String: Any]) -> ReplicatorConfiguration {
        guard let authenticatorData = data["authenticator"] as? [String: Any],
              let target = data["target"] as? [String: Any],
              let url = target["url"] as? String,
              let replicatorType = data["replicatorType"] as? String,
              let continuous = data["continuous"] as? Bool else {
            fatalError("Invalid JSON data")
        }

        let endpoint = URLEndpoint(url: URL(string: url)!)
        var replConfig = ReplicatorConfiguration(database: database, target: endpoint)

        switch replicatorType {
        case "PUSH_AND_PULL":
            replConfig.replicatorType = .pushAndPull
        case "PULL":
            replConfig.replicatorType = .pull
        case "PUSH":
            replConfig.replicatorType = .push
        default:
            fatalError("Invalid replicatorType")
        }

        if let channels = data["channels"] as? [String] {
            replConfig.channels = channels
        }

        replConfig.continuous = continuous

        if let authenticator = ReplicatorHelper.replicatorAuthenticatorFromConfig(authenticatorData) {
            replConfig.authenticator = authenticator
        }

        return replConfig
    }

    private static func replicatorCollectionConfigFromJson(_ data: [String: Any]) -> CollectionConfiguration {
        let config = CollectionConfiguration()

        return config
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
