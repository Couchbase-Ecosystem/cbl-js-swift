//
//  CustomQuery.m
//  CbliteSwiftJsLib
//
//  Created by Aaron LaBeau on 07/04/24.
//

#import <objc/runtime.h>
#import <Foundation/Foundation.h>

#import "CustomQuery.h"

@implementation CustomQuery

-(instancetype) initWithJson:(NSData *)jsonData database:(CBLDatabase*)database {
    SEL sel = NSSelectorFromString(@"initWithDatabase:JSONRepresentation:");
    id queryInstance = [CBLQuery alloc];
    
    Ivar ivar = class_getInstanceVariable(CBLQuery.self, "_from");
    object_setIvar(queryInstance, ivar, [CBLQueryDataSource database:database]);
    
    id (*method)(id, SEL, id, id) = (void *)[queryInstance methodForSelector:sel];
    return method(queryInstance, sel, database, jsonData);
}

@end
