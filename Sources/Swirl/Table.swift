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

public protocol Table : Named, Dataset, ErasedRep {
    var columns:Columns {get}
}

public extension Table {
    public var tables:[Table] {
        return [self]
    }
}

public struct ErasedTable : Table, QueryLike, Rep {
    public typealias DS = ErasedTable
    public typealias Ret = ErasedTable
    public typealias Value = ErasedTable
    
    public let name:String
    public let columns:Columns
    
    public init(name:String, columns:Columns = .all) {
        self.name = name
        self.columns = columns
    }
    
    public subscript(_ column:String) -> ErasedColumn {
        return ErasedColumn(name: column, in: self)
    }
    
    public func c<T>(_ column:String, type:T.Type = T.self) -> TypedColumn<T> {
        return self[column].bind(type)
    }
    
    public func filter(_ f: (Ret)->Predicate) -> QueryImpl<DS, Ret> {
        return QueryImpl(dataset: self, ret: self, predicate: f(self))
    }
}

public protocol TableProtocol : Table {
    associatedtype Tuple : RepRichTuple
    associatedtype Value
    
    static var table:String {get}
}

public extension TableProtocol {
    static func column<T>(_ name: String) -> TypedColumn<T> {
        return TypedColumn(name: name, in: ErasedTable(name: Self.table))
    }
}

open class TypedTable<E : EntityLike> : TableProtocol, Rep {
    public typealias Entity = E
    public typealias Tuple = Entity.Tuple
    public typealias ColumnsRep = Tuple.ColumnsRep
    public typealias Value = Entity
    
    public let name:String
    public let columns: Columns
    
    public let all:ColumnsRep
    
    public init(all:ColumnsRep.Tuple.Wrapped) {
        self.name = type(of: self).table
        self.all = Tuple.columns(all)
        self.columns = .list(self.all.stripe.flatMap { $0 as? Column }.map {$0.name})
    }
    
    public var stripe: [ErasedRep] {
        return all.stripe
    }
    
    open class var table: String {
        fatalError()
    }
}

public extension TableProtocol where Self : QueryLike, Self.DS == Self, Self.Ret == Self {
    public func filter(_ f: (Ret)->Predicate) -> QueryImpl<Self, Ret> {
        return QueryImpl(dataset: self, ret: self, predicate: f(self))
    }
}

extension ErasedTable {
    init(_ table:Table) {
        self.init(name: table.name, columns: table.columns)
    }
}
