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

/*extension SwirlOperation where Ret : Sequence {
    static func result<Q : QueryLike>(query: Q, parse:@escaping ([Any?])->Ret) -> SwirlOperation where Q.Ret : Demo.Ret, Ret.Iterator.Element == Q.Ret.Res {
        
    }
}*/

private extension QueryLike where Ret : Demo.Rep, Ret.Value : EntityLike {
    func select(parse:@escaping ([Any?])->Ret.Value.Bind) -> SwirlOperation<[Ret.Value.Bind]> {
        let select = query.select
        return SwirlOperation { swirl in
            return swirl.execute(sql: select(swirl)).flatMap{$0}.flatMap { results in
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

extension QueryLike {
    var query:QueryImpl<DS, Ret> {
        return self.map {$0}
    }
    
    var select:(Swirl) -> SQL {
        return { swirl in
            self.query.render(dialect: swirl.dialect)
        }
    }
    
    func insert(item:Ret) -> (Swirl) -> SQL {
        return { swirl in
            fatalError()
        }
    }
}

/*public extension Query where Ret : TupleRepProtocol {
    public var result:SwirlOperation<[Ret.Res]> {
        return self.select { array -> Ret.Res in
            Ret.parse(array: array) as! Ret.Res
        }
    }
}*/

public extension QueryLike where Ret : Rep, Ret.Value : EntityLike {
    public var result:SwirlOperation<[Ret.Value.Bind]> {
        return self.query.select(parse: Ret.Value.parse)
    }
}
