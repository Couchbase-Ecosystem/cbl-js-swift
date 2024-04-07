//
//  ProcessResults.swift
//  CbliteSwiftJsLib
//
//  Created by Aaron LaBeau on 4/7/24.
//

import Foundation

public struct ProcessResults<T> {
    var errorMessage: String
    var results: T?
    
    init () {
        self.errorMessage = ""
    }
    
    init(errorMessage: String, results: T? = nil) {
        self.errorMessage = errorMessage
        self.results = results
    }
}
