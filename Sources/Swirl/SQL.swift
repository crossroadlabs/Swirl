//===--- SQL.swift ------------------------------------------------------===//
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

import Foundation

public struct SQL {
    public let query:String
    public let parameters:[Any?]
    
    public init(query:String, parameters:[Any?]) {
        self.query = query
        self.parameters = parameters
    }
}

public func +(a:SQL, b:SQL) -> SQL {
    return SQL(query: a.query + b.query, parameters: a.parameters + b.parameters)
}

public func +(a:SQL, b:String) -> SQL {
    return SQL(query: a.query + b, parameters: a.parameters)
}

public func +(a:String, b:SQL) -> SQL {
    return SQL(query: a + b.query, parameters: b.parameters)
}

public extension Sequence where Iterator.Element == SQL {
    public func joined(separator: String = "") -> SQL {
        let strings = self.map {$0.query}
        let query = strings.joined(separator: separator)
        let params = self.flatMap {$0.parameters}
        return SQL(query: query, parameters: params)
    }
}
