//===--- Function.swift ------------------------------------------------------===//
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

public enum ForeignFunctionName {
    case uppercase
    case lowercase
    
    case custom(name: String)
}

public protocol ForeignFunctionProtocol : Rep {
    var name: ForeignFunctionName {get}
    var args: [ErasedRep] {get}
}

public struct ForeignFunction0<R> : ForeignFunctionProtocol {
    public typealias Value = R
    
    public let name: ForeignFunctionName
    public let args: [ErasedRep] = []
    
    public init(name: ForeignFunctionName) {
        self.name = name
    }
}

public protocol TupledForeignFunctionProtocol : ForeignFunctionProtocol {
    associatedtype Tuple : TupleProtocol
    
    var tuple: Tuple {get}
}

public extension ForeignFunctionProtocol where Self : TupledForeignFunctionProtocol {
    public var args: [ErasedRep] {
        return tuple.stripe.flatMap {$0 as? ErasedRep}
    }
}

public struct ForeignFunction1<A : Rep, R> : TupledForeignFunctionProtocol {
    public typealias Value = R
    public typealias Tuple = Tuple1<A>
    
    public let name: ForeignFunctionName
    public let tuple: Tuple
    
    public init(name: ForeignFunctionName, _ a: A) {
        self.name = name
        self.tuple = Tuple(a)
    }
}

public struct ForeignFunction2<A : Rep, B : Rep, R> : TupledForeignFunctionProtocol {
    public typealias Value = R
    public typealias Tuple = Tuple2<A, B>
    
    public let name: ForeignFunctionName
    public let tuple: Tuple
    
    public init(name: ForeignFunctionName, _ a: A, _ b: B) {
        self.name = name
        self.tuple = Tuple(a, b)
    }
}

//////////////////////////////////////////// CONCRETE FUNCTIONS ////////////////////////////////////////////

public extension Rep where Value == String {
    public func uppercased() -> ForeignFunction1<Self, String> {
        return ForeignFunction1(name: .uppercase, self)
    }
    
    public func lowercased() -> ForeignFunction1<Self, String> {
        return ForeignFunction1(name: .lowercase, self)
    }
}

public extension ErasedColumn {
    public func uppercased() -> ForeignFunction1<TypedColumn<String>, String> {
        return self.bind(String.self).uppercased()
    }
    
    public func lowercased() -> ForeignFunction1<TypedColumn<String>, String> {
        return self.bind(String.self).uppercased()
    }
}




