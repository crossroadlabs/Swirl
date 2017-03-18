//===--- SyncInterface.swift ------------------------------------------------------===//
//Copyright (c) 2017 Crossroad Labs s.r.o.
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.
//===----------------------------------------------------------------------===//

public protocol SyncDriver {
    var proto:String {get}
    
    func connect(url:String, params:Dictionary<String, String>) throws -> SyncConnection
}

public protocol SyncConnection {
    @discardableResult
    func execute(query:String, parameters: [Any?], named: [String:Any?]) throws -> SyncResultSet?
}

public protocol SyncResultSet {
    func columnCount() throws -> Int
    func columns() throws -> [String]
    
    func reset() throws
    func next() throws -> [Any?]?
}
