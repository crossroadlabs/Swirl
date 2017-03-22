//===--- TupleRep.swift ------------------------------------------------------===//
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

public protocol TupleRepProtocol : ValueRepProtocol {
}

public extension TupleRepProtocol {
    public func render(dialect: Dialect, aliases: [String : String]) -> SQL {
        fatalError("Can not render tuple rep")
    }
}

////////////////////////////////////////////////// TWO //////////////////////////////////////////////////

public protocol Tuple2RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype Value = (A, B)
}

public extension Tuple2RepProtocol {
    var tuple:(A, B) {
        return value as! (A, B)
    }
}

public extension Tuple2RepProtocol {
    public var stripe: [ErasedRep] {
        let t = tuple
        let s:[ErasedRep] = [t.0, t.1]
        return s.flatMap {$0.stripe}
    }
}

public struct Tuple2Rep<AI : Rep, BI : Rep> : Tuple2RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias Value = (A, B)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B) {
        self.init(value: (a, b))
    }
}

////////////////////////////////////////////////// THREE //////////////////////////////////////////////////

public protocol Tuple3RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype C : Rep
    associatedtype Value = (A, B, C)
}

public extension Tuple3RepProtocol {
    var tuple:(A, B, C) {
        return value as! (A, B, C)
    }
}

public extension Tuple3RepProtocol {
    public var stripe: [ErasedRep] {
        let t = tuple
        let s:[ErasedRep] = [t.0, t.1, t.2]
        return s.flatMap {$0.stripe}
    }
}

public struct Tuple3Rep<AI : Rep, BI : Rep, CI : Rep> : Tuple3RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias C = CI
    public typealias Value = (A, B, C)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B, _ c:C) {
        self.init(value: (a, b, c))
    }
}

////////////////////////////////////////////////// FOUR //////////////////////////////////////////////////

public protocol Tuple4RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype C : Rep
    associatedtype D : Rep
    associatedtype Value = (A, B, C, D)
}

public extension Tuple4RepProtocol {
    var tuple:(A, B, C, D) {
        return value as! (A, B, C, D)
    }
}

public extension Tuple4RepProtocol {
    public var stripe: [ErasedRep] {
        let t = tuple
        let s:[ErasedRep] = [t.0, t.1, t.2, t.3]
        return s.flatMap {$0.stripe}
    }
}

public struct Tuple4Rep<AI : Rep, BI : Rep, CI : Rep, DI : Rep> : Tuple4RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias C = CI
    public typealias D = DI
    public typealias Value = (A, B, C, D)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D) {
        self.init(value: (a, b, c, d))
    }
}

////////////////////////////////////////////////// FIVE //////////////////////////////////////////////////

public protocol Tuple5RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype C : Rep
    associatedtype D : Rep
    associatedtype E : Rep
    associatedtype Value = (A, B, C, D, E)
}

public extension Tuple5RepProtocol {
    var tuple:(A, B, C, D, E) {
        return value as! (A, B, C, D, E)
    }
}

public extension Tuple5RepProtocol {
    public var stripe: [ErasedRep] {
        let t = tuple
        let s:[ErasedRep] = [t.0, t.1, t.2, t.3, t.4]
        return s.flatMap {$0.stripe}
    }
}

public struct Tuple5Rep<AI : Rep, BI : Rep, CI : Rep, DI : Rep, EI : Rep> : Tuple5RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias C = CI
    public typealias D = DI
    public typealias E = EI
    public typealias Value = (A, B, C, D, E)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D, _ e:E) {
        self.init(value: (a, b, c, d, e))
    }
}

////////////////////////////////////////////////// SIX //////////////////////////////////////////////////

public protocol Tuple6RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype C : Rep
    associatedtype D : Rep
    associatedtype E : Rep
    associatedtype F : Rep
    associatedtype Value = (A, B, C, D, E, F)
}

public extension Tuple6RepProtocol {
    var tuple:(A, B, C, D, E, F) {
        return value as! (A, B, C, D, E, F)
    }
}

public extension Tuple6RepProtocol {
    public var stripe: [ErasedRep] {
        let t = tuple
        let s:[ErasedRep] = [t.0, t.1, t.2, t.3, t.4, t.5]
        return s.flatMap {$0.stripe}
    }
}

public struct Tuple6Rep<AI : Rep, BI : Rep, CI : Rep, DI : Rep, EI : Rep, FI : Rep> : Tuple6RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias C = CI
    public typealias D = DI
    public typealias E = EI
    public typealias F = FI
    public typealias Value = (A, B, C, D, E, F)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D, _ e:E, _ f:F) {
        self.init(value: (a, b, c, d, e, f))
    }
}
