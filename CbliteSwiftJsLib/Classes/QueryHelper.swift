//
//  QueryHelper.swift
//  CbliteSwiftJsLib
//
//  Created by Aaron LaBeau on 08/04/24.
//

import Foundation
import CouchbaseLiteSwift

enum QueryError: Error {
    case invalidParameter(message: String)
    case unknownError(message: String)
}

public struct QueryHelper {

    public static func getParamatersFromJson(_ data: [String: Any])
       throws -> Parameters? {
        var parameters = Parameters()
        for (key, value) in data {
            // Ensure the value is a dictionary
            guard let innerDictionary = value as? [String: Any] else {
                continue
            }

            // Extract type and value from the inner dictionary
            if let type = innerDictionary["type"] as? String,
               let value = innerDictionary["value"] {

                // switch through types adding the parameters
                switch type {
                case "string":
                    if let stringValue = value as? String {
                        parameters.setString(stringValue, forName: key)
                    }
                case "float":
                    if let floatValue = value as? Float {
                        parameters.setFloat(floatValue, forName: key)
                    }
                case "boolean":
                    if let booleanValue = value as? Bool {
                        parameters.setBoolean(booleanValue, forName: key)
                    }
                case "double":
                    if let doubleValue = value as? Double {
                        parameters.setDouble(doubleValue, forName: key)
                    }
                case "date":
                    if let dateValue = value as? Date {
                        parameters.setDate(dateValue, forName: key)
                    }
                case "int":
                    if let intValue = value as? Int {
                        parameters.setInt(intValue, forName: key)
                    }
                case "int64":
                    if let int64Value = value as? Int64 {
                        parameters.setInt64(int64Value, forName: key)
                    }
                default:
                    throw QueryError.invalidParameter(message: type)
                }
            }
        }
        return parameters
    }
}
