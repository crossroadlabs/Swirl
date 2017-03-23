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
    
    var tuple: Wrapped {get}
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
}

public protocol CaseProtocol {
    associatedtype Tuple : Demo.Tuple
    
    init(tuple: Tuple.Wrapped)
    
    var tuple: Self.Tuple.Wrapped {get}
}
