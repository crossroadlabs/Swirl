//===--- Column.swift ------------------------------------------------------===//
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

import Boilerplate

public enum Columns {
    case list([String])
    case all
}

public protocol Column : Named, ErasedRep {
    var table:Table {get}
}

public struct ErasedColumn : Column, Rep {
    public typealias Value = Any
    
    public let name:String
    public let table:Table
    
    init(name: String, in table: Table) {
        self.name = name
        self.table = table
    }
}

public extension Column {
    public func render(dialect: Dialect, aliases: [String : String]) -> SQL {
        let table = aliases[self.table.name] ?? self.table.name
        return dialect.render(column: name, table: table, escape: true)
    }
}
