//===--- Rep.swift ------------------------------------------------------===//
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

public protocol ErasedRep {
    var stripe: [ErasedRep] {get}
    
    func render(dialect:Dialect, aliases:[String: String]) -> SQL
}

public extension ErasedRep {
    public var stripe: [ErasedRep] {
        return [self]
    }
}

public protocol Rep : ErasedRep {
    associatedtype Value
}

public protocol ValueRepProtocol : Rep {
    var value:Value {get}
}

public struct ValueRep<T> : ValueRepProtocol {
    public typealias Value = T?
    
    public let value: Value
    
    public init(value:Value) {
        self.value = value
    }
    
    public init<VR : ValueRepProtocol>(rep:VR) where VR.Value == Value {
        self.init(value: rep.value)
    }
}

public extension ValueRep {
    public func render(dialect: Dialect, aliases: [String : String]) -> SQL {
        return dialect.render(value: value)
    }
}
