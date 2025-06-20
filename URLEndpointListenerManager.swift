//
//  URLEndpointListenerManager.swift
//  CbliteSwiftJsLib
//

import Foundation
import CouchbaseLiteSwift

enum URLEndpointListenerError: Error {
    
    case unableToFindListener(listenerId: String)
    case databaseNotFound(databaseName: String)
}

public class URLEndpointListenerManager {
    private var listeners = [String: URLEndpointListener]()

    // Singleton instance
    public static let shared = URLEndpointListenerManager()

    private init() {}

    public func createListener(
        collections: [Collection],
        port: UInt16? = nil,
        tlsIdentity: TLSIdentity? = nil,
        networkInterface: String? = nil,
        disableTLS: Bool? = nil,
        enableDeltaSync: Bool? = nil,
        authenticatorConfig: [String: Any]? = nil
    ) throws -> String {
        var config = URLEndpointListenerConfiguration(collections: collections)
        if let port = port {
            config.port = port
        }
        if let networkInterface = networkInterface {
            config.networkInterface = networkInterface
        }
        if let disableTLS = disableTLS {
            config.disableTLS = disableTLS
        }
        if let enableDeltaSync = enableDeltaSync {
            config.enableDeltaSync = enableDeltaSync
        }
        config.tlsIdentity = nil 

        if let authenticator = Self.listenerAuthenticatorFromConfig(authenticatorConfig) {
            config.authenticator = authenticator
        }

        let listener = URLEndpointListener(config: config)
        let listenerId = UUID().uuidString
        listeners[listenerId] = listener

        return listenerId
    }

    public func startListener(listenerId: String) throws {
        guard let listener = listeners[listenerId] else {
            throw URLEndpointListenerError.unableToFindListener(listenerId: listenerId)
        }

        try listener.start()
    }

    public func stopListener(listenerId: String) throws {
        guard let listener = listeners[listenerId] else {
            throw URLEndpointListenerError.unableToFindListener(listenerId: listenerId)
        }

        listener.stop()
    }

    public func getListenerStatus(listenerId: String) throws -> CouchbaseLiteSwift.URLEndpointListener.ConnectionStatus {
        guard let listener = listeners[listenerId] else {
            throw URLEndpointListenerError.unableToFindListener(listenerId: listenerId)
        }

        return listener.status
    }

    public func getListenerUrls(listenerId: String) throws -> [String] {
        guard let listener = listeners[listenerId] else {
            throw URLEndpointListenerError.unableToFindListener(listenerId: listenerId)
        }

        return listener.urls?.map { $0.absoluteString } ?? []
    }
private static func listenerAuthenticatorFromConfig(_ config: [String: Any]?) -> ListenerAuthenticator? {
    guard let type = config?["type"] as? String,
          let data = config?["data"] as? [String: Any] else {
        return nil
    }
    switch type {
    case "basic":
        guard let username = data["username"] as? String,
              let password = data["password"] as? String else {
            return nil
        }
        return ListenerPasswordAuthenticator { (inputUsername, inputPassword) in
            return inputUsername == username && inputPassword == password
        }
    default:
        return nil
    }
}
}

