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

import protocol Boilerplate.TupleProtocol

public protocol ArrayParser {
    associatedtype ArrayParseResult
    
    static func parse(array:[Any?]) -> ArrayParseResult
}

public protocol RepRichTuple : TupleProtocol {
    associatedtype ColumnsRep : TupleRepProtocol
    
    static func columns(_ columns:ColumnsRep.Tuple.Wrapped) -> ColumnsRep
}

extension Tuple1 : RepRichTuple {
    public typealias ColumnsRep = Tuple1Rep<TypedColumn<A>>
    
    public static func columns(_ columns:ColumnsRep.Tuple.Wrapped) -> ColumnsRep {
        return ColumnsRep(tuple: columns)
    }
}

extension Tuple2 : RepRichTuple {
    public typealias ColumnsRep = Tuple2Rep<TypedColumn<A>, TypedColumn<B>>
    
    public static func columns(_ columns:ColumnsRep.Tuple.Wrapped) -> ColumnsRep {
        return ColumnsRep(tuple: columns)
    }
}

extension Tuple3 : RepRichTuple {
    public typealias ColumnsRep = Tuple3Rep<TypedColumn<A>, TypedColumn<B>, TypedColumn<C>>
    
    public static func columns(_ columns:ColumnsRep.Tuple.Wrapped) -> ColumnsRep {
        return ColumnsRep(tuple: columns)
    }
}

extension Tuple4 : RepRichTuple {
    public typealias ColumnsRep = Tuple4Rep<
        TypedColumn<A>,
        TypedColumn<B>,
        TypedColumn<C>,
        TypedColumn<D>>
    
    public static func columns(_ columns:ColumnsRep.Tuple.Wrapped) -> ColumnsRep {
        return ColumnsRep(tuple: columns)
    }
}

extension Tuple5 : RepRichTuple {
    public typealias ColumnsRep = Tuple3Rep<TypedColumn<A>, TypedColumn<B>, TypedColumn<C>>
    
    public static func columns(_ columns:ColumnsRep.Tuple.Wrapped) -> ColumnsRep {
        return ColumnsRep(tuple: columns)
    }
}

public protocol TupleRepProtocol : Rep {
    associatedtype Tuple : TupleProtocol
    associatedtype Naked
    typealias ArrayParseResult = Naked
    
    var tuple:Tuple {get}
    var wrapped:Tuple.Wrapped {get}
}

public extension TupleRepProtocol {
    public var wrapped: Tuple.Wrapped {
        return tuple.tuple
    }
    
    public var stripe: [ErasedRep] {
        return tuple.stripe.flatMap {$0 as? ErasedRep}.flatMap {$0.stripe}
    }
}

//EntityLike
/*public extension TupleRepProtocol {
    public typealias Bind = Naked
    
    public func rep() -> [ErasedRep] {
        return self.stripe
    }
}*/

////////////////////////////////////////////////// ONE //////////////////////////////////////////////////

public protocol Tuple1RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
}

public extension Tuple1RepProtocol {
    public typealias Tuple = Tuple1<A>
}

public struct Tuple1Rep<AI : Rep> : Tuple1RepProtocol {
    public typealias A = AI
    public typealias Tuple = Tuple1<A>
    public typealias Value = Tuple1<A.Value>
    public typealias Naked = Value.Wrapped
    
    public let tuple: Tuple1<A>
    
    public init(tuple: (A)) {
        self.tuple = Tuple1<A>(tuple: tuple)
    }
    
    public init(_ a:A) {
        self.init(tuple: (a))
    }
}

////////////////////////////////////////////////// TWO //////////////////////////////////////////////////

public protocol Tuple2RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
}

public extension Tuple2RepProtocol {
    public typealias Tuple = Tuple2<A, B>
}

public struct Tuple2Rep<AI : Rep, BI : Rep> : Tuple2RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias Tuple = Tuple2<A, B>
    public typealias Value = Tuple2<A.Value, B.Value>
    public typealias Naked = Value.Wrapped
    
    public let tuple: Tuple2<A, B>
    
    public init(tuple: (A, B)) {
        self.tuple = Tuple2<A, B>(tuple: tuple)
    }
    
    public init(_ a:A, _ b:B) {
        self.init(tuple: (a, b))
    }
}

////////////////////////////////////////////////// THREE //////////////////////////////////////////////////

public protocol Tuple3RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype C : Rep
}

public extension Tuple3RepProtocol {
    public typealias Tuple = Tuple3<A, B, C>
}

public struct Tuple3Rep<AI : Rep, BI : Rep, CI : Rep> : Tuple3RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias C = CI
    public typealias Tuple = Tuple3<A, B, C>
    public typealias Value = Tuple3<A.Value, B.Value, C.Value>
    public typealias Naked = Value.Wrapped
    
    public let tuple: Tuple3<A, B, C>
    
    public init(tuple: (A, B, C)) {
        self.tuple = Tuple3<A, B, C>(tuple: tuple)
    }
    
    public init(_ a:A, _ b:B, _ c:C) {
        self.init(tuple: (a, b, c))
    }
}

////////////////////////////////////////////////// FOUR //////////////////////////////////////////////////

public protocol Tuple4RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype C : Rep
    associatedtype D : Rep
}

public extension Tuple4RepProtocol {
    public typealias Tuple = Tuple4<A, B, C, D>
}

public struct Tuple4Rep<AI : Rep, BI : Rep, CI : Rep, DI : Rep> : Tuple4RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias C = CI
    public typealias D = DI
    public typealias Tuple = Tuple4<A, B, C, D>
    public typealias Value = Tuple4<A.Value, B.Value, C.Value, D.Value>
    public typealias Naked = Value.Wrapped
    
    public let tuple: Tuple4<A, B, C, D>
    
    public init(tuple: (A, B, C, D)) {
        self.tuple = Tuple4<A, B, C, D>(tuple: tuple)
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D) {
        self.init(tuple: (a, b, c, d))
    }
}

////////////////////////////////////////////////// FIVE //////////////////////////////////////////////////

public protocol Tuple5RepProtocol : TupleRepProtocol {
    associatedtype A : Rep
    associatedtype B : Rep
    associatedtype C : Rep
    associatedtype D : Rep
    associatedtype E : Rep
}

public extension Tuple5RepProtocol {
    public typealias Tuple = Tuple5<A, B, C, D, E>
}

public struct Tuple5Rep<AI : Rep, BI : Rep, CI : Rep, DI : Rep, EI : Rep> : Tuple5RepProtocol {
    public typealias A = AI
    public typealias B = BI
    public typealias C = CI
    public typealias D = DI
    public typealias E = EI
    public typealias Tuple = Tuple5<A, B, C, D, E>
    public typealias Value = Tuple5<A.Value, B.Value, C.Value, D.Value, E.Value>
    public typealias Naked = Value.Wrapped
    
    public let tuple: Tuple5<A, B, C, D, E>
    
    public init(tuple: (A, B, C, D, E)) {
        self.tuple = Tuple5<A, B, C, D, E>(tuple: tuple)
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D, _ e:E) {
        self.init(tuple: (a, b, c, d, e))
    }
}

////////////////////////////////////////////////// FOUR //////////////////////////////////////////////////
/*
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
    public typealias Tuple = Tuple4<A, B, C, D>
    public typealias Value = (A, B, C, D)
    public typealias Naked = (A.Value, B.Value, C.Value, D.Value)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D) {
        self.init(value: (a, b, c, d))
    }
}

public extension Tuple4Rep {
    static func parse(array:[Any?]) -> Naked {
        return (array[0]! as! A.Value, array[1]! as! B.Value, array[2]! as! C.Value, array[3]! as! D.Value)
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
    public typealias Naked = (A.Value, B.Value, C.Value, D.Value, E.Value)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D, _ e:E) {
        self.init(value: (a, b, c, d, e))
    }
}

public extension Tuple5Rep {
    static func parse(array:[Any?]) -> Naked {
        return (array[0]! as! A.Value, array[1]! as! B.Value, array[2]! as! C.Value, array[3]! as! D.Value, array[4]! as! E.Value)
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
    public typealias Naked = (A.Value, B.Value, C.Value, D.Value, E.Value, F.Value)
    
    public let value:Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init(_ a:A, _ b:B, _ c:C, _ d:D, _ e:E, _ f:F) {
        self.init(value: (a, b, c, d, e, f))
    }
}

public extension Tuple6Rep {
    static func parse(array:[Any?]) -> Naked {
        return (array[0]! as! A.Value,
                array[1]! as! B.Value,
                array[2]! as! C.Value,
                array[3]! as! D.Value,
                array[4]! as! E.Value,
                array[5]! as! F.Value)
    }
}*/
