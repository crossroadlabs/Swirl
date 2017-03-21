//===--- Table.swift ------------------------------------------------------===//
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

public protocol Table : Named, Dataset {
    var columns:Columns {get}
    
    init(name:String, columns:Columns)
    
    subscript(_ column:String) -> Column {get}
}

public extension Table {
    public var tables:[Table] {
        return [self]
    }
}

public extension Table {
    public func render(dialect:Dialect, aliases: [String: String]) -> SQL {
        return dialect.render(table: self, aliases: aliases)
    }
}

public struct ErasedTable : Table {
    public let name:String
    public let columns:Columns
    
    public init(name:String, columns:Columns = .all) {
        self.name = name
        self.columns = columns
    }
    
    public subscript(_ column:String) -> Column {
        return ErasedColumn(name: column, in: self)
    }
}

extension ErasedTable {
    init(_ table:Table) {
        self.init(name: table.name, columns: table.columns)
    }
}
