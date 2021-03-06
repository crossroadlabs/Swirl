//===--- SwirlOperation.swift ------------------------------------------------------===//
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

import Future
import Event

import protocol RDBC.ResultSet

public protocol SwirlOperationProtocol {
    associatedtype Ret
    
    func execute(in swirl:Swirl) -> Future<Ret>
}

public class SwirlOperation<RetI> : SwirlOperationProtocol {
    public typealias Ret = RetI
    public typealias SwirlOp = (Swirl) -> Future<Ret>
    
    private let _op:SwirlOp
    
    public init(_ f:@escaping SwirlOp) {
        _op = f
    }
    
    public func execute(in swirl:Swirl) -> Future<Ret> {
        return _op(swirl)
    }
}

public func <=<SO : SwirlOperationProtocol>(swirl:Swirl, operation: SO) -> Future<SO.Ret> {
    return operation.execute(in: swirl)
}

public func =><SO : SwirlOperationProtocol>(operation: SO, swirl:Swirl) -> Future<SO.Ret> {
    return operation.execute(in: swirl)
}

public extension Swirl {
    public func execute<Ret, SO : SwirlOperationProtocol>(_ operation: SO) -> Future<Ret> where SO.Ret == Ret {
        return operation.execute(in: self)
    }
    
    public func execute<Ret, SO : SwirlOperationProtocol>(_ operations: [SO]) -> Future<[Ret]> where SO.Ret == Ret {
        //we can't do traverse here. This shit must be sequencial
        guard let head = operations.first else {
            return Future(value: [])
        }
        
        let tail = operations.dropFirst()
        let swirl = self.sequencial
        return swirl.flatMap { swirl in
            head.execute(in: swirl).flatMap { result in
                swirl.execute(Array(tail)).map { rest in
                    [result] + rest
                }
            }
        }
    }
}

public extension Sequence where Iterator.Element : SwirlOperationProtocol {
    public func execute(in swirl: Swirl) -> Future<[Iterator.Element.Ret]> {
        return swirl.execute(Array(self))
    }
}

private extension QueryLike where Ret.Value : EntityLike {
    func select(parse:@escaping ([Any?])->Ret.Value.Bind) -> SwirlOperation<[Ret.Value.Bind]> {
        let select = self.select
        return SwirlOperation { swirl in
            swirl.execute(renderlet: select).flatMap{$0}.flatMap { results in
                results.rows()
            }.map { /*(cols,*/ rows/*)*/ in
                rows.map(parse)
            }.recover { (e:FutureError) in
                switch e {
                    case .mappedNil:
                        return []
                    default:
                        throw e
                }
            }
        }
    }
}

public extension QueryLike where Ret.Value : EntityLike {
    public var result:SwirlOperation<[Ret.Value.Bind]> {
        return self.select(parse: Ret.Value.parse)
    }
}

infix operator ++= : AssignmentPrecedence
infix operator ?= : AssignmentPrecedence
infix operator ?+= : AssignmentPrecedence

private enum UpsertException : Error {
    case notUpdated
}

public extension QueryLike where Ret.Value : EntityLike, DS : Table {
    private static func count<F: FutureProtocol>(from: F, swirl: Swirl) -> Future<Int> where F.Value == ResultSet? {
        return from.flatMap {$0}.flatMap { rs in
            rs.dictionaries()
        }.flatMap { dictionaries in
            dictionaries.first.flatMap { row in
                swirl.execute { dialect in
                    row[dialect.affected].flatMap {$0 as? Int}
                }
            }
        }.recover { (e:FutureError) in
            switch e {
                case .mappedNil:
                    return 0
                default:
                    throw e
            }
        }
    }
    
    private func insert(renderlet: @escaping Renderlet) -> SwirlOperation<Int> {
        return SwirlOperation { swirl in
            Self.count(from: swirl.execute(renderlet: renderlet), swirl: swirl)
        }
    }
    
    public func insert(item: Ret.Value.Bind) -> SwirlOperation<Int> {
        return insert(renderlet: insertlet(item: item))
    }
    
    public func insert(items: [Ret.Value.Bind]) -> SwirlOperation<Int> {
        return insert(renderlet: insertlet(items: items))
    }
    
    public func update(with values: Ret.Value.Bind) -> SwirlOperation<Int> {
        let renderlet = updatelet(with: values)
        return SwirlOperation { swirl in
            Self.count(from: swirl.execute(renderlet: renderlet), swirl: swirl)
        }
    }
    
    public var delete: SwirlOperation<Int> {
        let renderlet: Renderlet = self.deletelet
        return SwirlOperation { swirl in
            Self.count(from: swirl.execute(renderlet: renderlet), swirl: swirl)
        }
    }
    
    public func upsert(item: Ret.Value.Bind) -> SwirlOperation<Int> {
        let updatelet = self.updatelet(with: item)
        
        return SwirlOperation { swirl in
            swirl.sequencial.flatMap { swirl in
                Self.count(from: swirl.execute(renderlet: updatelet), swirl: swirl).map { count in
                    if count > 0 {
                        return count
                    }
                    throw UpsertException.notUpdated
                }.recoverWith { (e:UpsertException) in
                    self.insert(item: item).execute(in: swirl)
                }
            }
        }
    }
    
    public static func +=(q:Self, item: Ret.Value.Bind) -> SwirlOperation<Int> {
        return q.insert(item: item)
    }
    
    //TODO: move all operators to Boilerplate
    public static func ++=(q:Self, items: [Ret.Value.Bind]) -> SwirlOperation<Int> {
        return q.insert(items: items)
    }
    
    public static func ?=(q:Self, item: Ret.Value.Bind) -> SwirlOperation<Int> {
        return q.update(with: item)
    }
    
    public static func ?+=(q:Self, item: Ret.Value.Bind) -> SwirlOperation<Int> {
        return q.upsert(item: item)
    }
}
