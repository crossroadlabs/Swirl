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

public class SwirlOperation<Ret> {
    public typealias SwirlOp = (Swirl) -> Future<Ret>
    
    private let _op:SwirlOp
    
    public init(_ f:@escaping SwirlOp) {
        _op = f
    }
    
    public func execute(in swirl:Swirl) -> Future<Ret> {
        return _op(swirl)
    }
}

public extension SwirlOperation {
    public static func <=(swirl:Swirl, operation:SwirlOperation) -> Future<Ret> {
        return operation.execute(in: swirl)
    }
    
    public static func =>(operation:SwirlOperation, swirl:Swirl) -> Future<Ret> {
        return operation.execute(in: swirl)
    }
}

public extension Swirl {
    public func execute<Ret>(_ operation: SwirlOperation<Ret>) -> Future<Ret> {
        return operation.execute(in: self)
    }
    
    public func execute<Ret>(_ operations: [SwirlOperation<Ret>]) -> Future<[Ret]> {
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

private extension QueryLike where Ret.Value : EntityLike {
    func select(parse:@escaping ([Any?])->Ret.Value.Bind) -> SwirlOperation<[Ret.Value.Bind]> {
        let select = query.select
        return SwirlOperation { swirl in
            swirl.execute(renderlet: select).flatMap{$0}.flatMap { results in
                results.all()
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
        return self.query.select(parse: Ret.Value.parse)
    }
}

public extension QueryLike where Ret.Value : EntityLike, DS : TableProtocol {
    public func insert(item: Ret.Value.Bind) -> SwirlOperation<Void> {
        let insert: Renderlet = self.query.insert(item: item)
        
        return SwirlOperation { swirl in
            swirl.execute(renderlet: insert).map {_ in ()}
        }
    }
    
    public func insert(items: [Ret.Value.Bind]) -> SwirlOperation<Void> {
        let insert: Renderlet = self.query.insert(items: items)
        
        return SwirlOperation { swirl in
            swirl.execute(renderlet: insert).map {_ in ()}
        }
    }
    
    public static func +=(q:Self, item: Ret.Value.Bind) -> SwirlOperation<Void> {
        return q.insert(item: item)
    }
    
    //TODO: implement ++= operator
    //TODO: move all operators to Boilerplate
    public static func +=(q:Self, items: [Ret.Value.Bind]) -> SwirlOperation<Void> {
        return q.insert(items: items)
    }
}

extension QueryLike {
    var query:QueryImpl<DS, Ret> {
        return self.map {$0}
    }
    
    var select: Renderlet {
        return { dialect in
            self.query.render(dialect: dialect)
        }
    }
}

extension QueryLike where Ret.Value : EntityLike, DS : TableProtocol {
    fileprivate func insert(item: Ret.Value.Bind) -> Renderlet {
        let q = self.query
        let rep = Ret.Value.unbind(bound: item).rep()
        return { dialect in
            dialect.render(insert: rep, to: q.dataset, ret: q.ret)
        }
    }
    
    fileprivate func insert(items: [Ret.Value.Bind]) -> Renderlet {
        let q = self.query
        let reps = items.map(Ret.Value.unbind).map{$0.rep()}
        return { dialect in
            dialect.render(insert: reps, to: q.dataset, ret: q.ret)
        }
    }
}
