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

public protocol EntityLike : ArrayParser {
    associatedtype Tuple : RepRichTuple
}

public protocol Entity : CaseProtocol, EntityLike {
}

public extension Entity {
    public typealias ArrayParseResult = Self
    
    public static func parse(array:[Any?]) -> Self {
        return Self(tuple: Tuple.ColumnsRep.parse(array: array) as! Tuple.Wrapped)
    }
}

public protocol TupleEntity : EntityLike {
}

public extension TupleEntity where Self : Demo.RepRichTuple {
    public typealias ArrayParseResult = Self.ColumnsRep.Naked
    
    public var wrapper: Self {
        return self
    }
    
    public init(wrapper t: Self) {
        self.init(tuple: t.tuple)
    }
    
    public static func parse(array:[Any?]) -> Self.ColumnsRep.Naked {
        return Self.ColumnsRep.parse(array: array)
    }
}

extension Tuple2 : TupleEntity {
    public typealias Tuple = Tuple2
}

extension Tuple3 : TupleEntity {
    public typealias Tuple = Tuple3
}
