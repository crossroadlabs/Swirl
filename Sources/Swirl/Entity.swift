//===--- Entity.swift ------------------------------------------------------===//
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

import protocol Boilerplate.Tuple
import protocol Boilerplate.CaseProtocol

public protocol EntityLike {
    associatedtype Tuple : RepRichTuple
    associatedtype Bind
    
    static func parse(array: [Any?]) -> Bind
    static func unbind(bound: Bind) -> Self
    func rep() -> [ErasedRep]
}

public protocol Entity : CaseProtocol, EntityLike {
}

public extension Entity {
    public typealias ArrayParseResult = Self
    public typealias Bind = Self
    
    public static func parse(array: [Any?]) -> Self {
        return Self(tuple: Tuple(array: array))
    }
    
    public static func unbind(bound: Self) -> Self {
        return bound
    }
    
    public func rep() -> [ErasedRep] {
        return tuple.stripe.map(ValueRep.init)
    }
}

public protocol TupleEntity : EntityLike, Tuple {
    associatedtype Bind : Wrapped
}

public extension TupleEntity where Self : RepRichTuple, Bind == Wrapped {
    public typealias ArrayParseResult = Self.Wrapped
    
    public var wrapper: Self {
        return self
    }
    
    public init(wrapper t: Self) {
        self.init(tuple: t.tuple)
    }
    
    public func rep() -> [ErasedRep] {
        return stripe.map(ValueRep.init)
    }
    
    public static func unbind(bound: Bind) -> Self {
        return Self(tuple: bound)
    }
    
    public static func parse(array:[Any?]) -> Self.Wrapped {
        return Self(array: array).tuple
    }
}

extension Tuple1 : TupleEntity {
    public typealias Tuple = Tuple1
    public typealias Bind = Tuple.Wrapped
}

extension Tuple2 : TupleEntity {
    public typealias Tuple = Tuple2
    public typealias Bind = Tuple.Wrapped
}

extension Tuple3 : TupleEntity {
    public typealias Tuple = Tuple3
    public typealias Bind = Tuple.Wrapped
}

extension Tuple4 : TupleEntity {
    public typealias Tuple = Tuple4
    public typealias Bind = Tuple.Wrapped
}

extension Tuple5 : TupleEntity {
    public typealias Tuple = Tuple5
    public typealias Bind = Tuple.Wrapped
}
