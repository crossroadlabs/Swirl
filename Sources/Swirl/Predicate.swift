//===--- Predicate.swift ------------------------------------------------------===//
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

public indirect enum Predicate {
    case null
    case bool(Bool)
    case compound(op:BinaryOp, Predicate, Predicate)
    case comparison(op:BinaryOp, a:ErasedRep, b:ErasedRep)
}

extension Predicate : ExpressibleByBooleanLiteral {
    public init(booleanLiteral value: Bool) {
        self = .bool(value)
    }
}

extension Predicate : ExpressibleByNilLiteral {
    public init(nilLiteral: ()) {
        self = .null
    }
}

extension Predicate {
    func map<B>(_ f: (Predicate)->B) -> B? {
        switch self {
        case .null:
            return nil
        default:
            return f(self)
        }
    }
}
