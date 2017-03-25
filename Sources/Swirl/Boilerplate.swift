//===--- Boilerplate.swift ------------------------------------------------------===//
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

public protocol Tuple {
    associatedtype Wrapped
    
    init(tuple: Wrapped)
    init(array: [Any?])
    
    var tuple: Wrapped {get}
    var stripe:[Any] {get}
}

public typealias TupleProtocol = Tuple

public protocol Tuple1Protocol : Tuple {
    associatedtype A
}

public struct Tuple1<AI> : Tuple1Protocol {
    public typealias A = AI
    public typealias Wrapped = (A)
    
    public let tuple: Wrapped
    
    public init(tuple: Wrapped) {
        self.tuple = tuple
    }
    
    public init(_ a: A) {
        self.init(tuple: (a))
    }
    
    public init(array: [Any?]) {
        self.init(array.first! as! A)
    }
    
    public var stripe:[Any] {
        return [tuple]
    }
}

public protocol Tuple2Protocol : Tuple {
    associatedtype A
    associatedtype B
}

public struct Tuple2<AI, BI> : Tuple2Protocol {
    public typealias A = AI
    public typealias B = BI
    public typealias Wrapped = (A, B)
    
    public let tuple: Wrapped
    
    public init(tuple: Wrapped) {
        self.tuple = tuple
    }
    
    public init(_ a: A, _ b: B) {
        self.init(tuple: (a, b))
    }
    
    public init(array: [Any?]) {
        self.init(array[0] as! A, array[1] as! B)
    }
    
    public var stripe:[Any] {
        return [tuple.0, tuple.1]
    }
}

public struct Tuple3<A, B, C> : Tuple {
    public typealias Wrapped = (A, B, C)
    
    public let tuple: Wrapped
    
    public init(tuple: Wrapped) {
        self.tuple = tuple
    }
    
    public init(_ a: A, _ b: B, _ c: C) {
        self.init(tuple: (a, b, c))
    }
    
    public init(array: [Any?]) {
        self.init(array[0] as! A, array[1] as! B, array[2] as! C)
    }
    
    public var stripe:[Any] {
        return [tuple.0, tuple.1, tuple.2]
    }
}

public protocol CaseProtocol {
    associatedtype Tuple : TupleProtocol
    
    init(tuple: Tuple.Wrapped)
    
    var tuple: Self.Tuple.Wrapped {get}
}
