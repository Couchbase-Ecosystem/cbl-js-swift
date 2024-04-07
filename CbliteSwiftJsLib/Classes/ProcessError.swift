//
//  ProcessError.swift
//  CbliteSwiftJsLib
//
//  Created by Aaron LaBeau on 4/7/24.
//

import Foundation

public struct ProcessError {
    var errorMessage: String
    
    init (_ errorMessage: String = "") {
        self.errorMessage = errorMessage
    }
}
