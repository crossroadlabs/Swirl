//===--- Query.swift ------------------------------------------------------===//
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

import Boilerplate

public protocol QueryLike {
    associatedtype DS : Dataset
    associatedtype Ret : Rep
    
    var query: QueryImpl<DS, Ret> {get}
}

public extension QueryLike where DS == Self, Ret == Self {
    var query: QueryImpl<DS, Ret> {
        return QueryImpl(dataset: self, ret: self)
    }
}

////////////////////////////////////////////////////// RENDERLETS //////////////////////////////////////////////////////
extension QueryLike {
    var select: Renderlet {
        return self.query.render
    }
}

extension QueryLike where Ret.Value : EntityLike, DS : Table {
    func insertlet(item: Ret.Value.Bind) -> Renderlet {
        let q = self.query
        let rep = Ret.Value.unbind(bound: item).rep()
        return { dialect in
            dialect.render(insert: rep, into: q.dataset, ret: q.ret)
        }
    }
    
    func insertlet(items: [Ret.Value.Bind]) -> Renderlet {
        let q = self.query
        let reps = items.map(Ret.Value.unbind).map{$0.rep()}
        return { dialect in
            dialect.render(insert: reps, into: q.dataset, ret: q.ret)
        }
    }
    
    func updatelet(with values: Ret.Value.Bind) -> Renderlet {
        let q = self.query
        let rep = Ret.Value.unbind(bound: values).rep()
        return { dialect in
            dialect.render(update: rep, into: q.dataset, ret: q.ret, matching: q.predicate)
        }
    }
    
    var deletelet: Renderlet {
        let q = self.query
        return { dialect in
            dialect.render(delete: q.dataset, matching: q.predicate)
        }
    }
}

//Bound Query
public protocol Query : QueryLike {
    var dataset:DS {get}
    var ret:Ret {get}
    var predicate:Predicate {get}
    var order:Any {get}
    var limit:Limit? {get}
}

private extension Query {
    func render(dialect:Dialect) -> SQL {
        return dialect.render(select: ret, from: dataset, filter: self.predicate, limit: limit)
    }
}

public struct QueryImpl<DSI : Dataset, RetI : Rep> : Query {
    public typealias DS = DSI
    public typealias Ret = RetI
    
    public let dataset:DS
    public let ret:Ret
    public let predicate:Predicate
    public let order:Any
    public let limit:Limit?
    
    init(dataset:DS, ret:Ret, predicate:Predicate = nil, order:Any = "", limit:Limit? = nil) {
        self.dataset = dataset
        self.ret = ret
        self.predicate = predicate
        self.order = order
        self.limit = limit
    }
}

public extension QueryImpl {
    public var query: QueryImpl<DS, Ret> {
        return self
    }
}

public typealias Q = QueryImpl<ErasedTable, ErasedTable>

public extension Query {
    public static func select(_ columns: [String]? = nil, from: String) -> QueryImpl<ErasedTable, ErasedTable> {
        let columns:Columns = columns.map { seq in
            .list(seq)
        }.getOr(else: .all)
        
        let table = ErasedTable(name: from, columns: columns)
        
        return QueryImpl(dataset: table,
                         ret: table,
                         predicate: nil,
                         order: "",
                         limit: nil)
    }
    
    public static func table(name: String) -> ErasedTable {
        return ErasedTable(name: name, columns: .all)
    }
}

public extension Query where DS : Table {
    public func select(_ columns: [String]? = nil) -> QueryImpl<DS, ErasedTable> {
        let columns:Columns = columns.map { seq in
            .list(seq)
        }.getOr(else: .all)
        
        return QueryImpl(dataset: dataset,
                         ret: ErasedTable(name: dataset.name, columns: columns),
                         predicate: predicate,
                         order: order,
                         limit: nil)
    }
}

private extension QueryLike {
    func _map<BRet : Rep>(_ f:(Ret)->BRet) -> QueryImpl<DS, BRet> {
        let q = self.query
        return QueryImpl(dataset: q.dataset, ret: f(q.ret), predicate: q.predicate, order: q.order, limit: q.limit)
    }
}

public extension QueryLike {
    public func map<A: Rep>(_ f:(Ret)->A) -> QueryImpl<DS, Tuple1Rep<A>> {
        return _map { ret in
            Tuple1Rep(tuple: f(ret))
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return _map { ret in
            Tuple2Rep(tuple: f(ret))
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return _map { ret in
            Tuple3Rep(tuple: f(ret))
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return _map { ret in
            Tuple4Rep(tuple: f(ret))
        }
    }
    
    public func map<A:Rep, B: Rep, C: Rep, D: Rep, E: Rep>(_ f:(Ret)->(A, B, C, D, E)) -> QueryImpl<DS, Tuple5Rep<A, B, C, D, E>> {
        return _map { ret in
            Tuple5Rep(tuple: f(ret))
        }
    }
}

public extension QueryLike where Ret : Tuple1RepProtocol, Ret.Tuple == Tuple1<Ret.A> {
    public func map<BRet : Rep>(_ f:(Ret.A)->BRet) -> QueryImpl<DS, BRet> {
        return _map { ret in
            ret.wrapped |> f
        }
    }
    
    public func filter(_ f:(Ret.A)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.wrapped |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return _map { ret in
            Tuple2Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return _map { ret in
            Tuple3Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return _map { ret in
            Tuple4Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep, E : Rep>(_ f:(Ret.A)->(A, B, C, D, E)) -> QueryImpl<DS, Tuple5Rep<A, B, C, D, E>> {
        return _map { ret in
            Tuple5Rep(tuple: ret.wrapped |> f)
        }
    }
}

public extension QueryLike where Ret : Tuple2RepProtocol, Ret.Tuple == Tuple2<Ret.A, Ret.B> {
    public func map<BRet : Rep>(_ f:(Ret.A, Ret.B)->BRet) -> QueryImpl<DS, BRet> {
        return _map { ret in
            ret.wrapped |> f
        }
    }
    
    public func filter(_ f:(Ret.A, Ret.B)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.wrapped |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A, Ret.B)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return _map { ret in
            Tuple2Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return _map { ret in
            Tuple3Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return _map { ret in
            Tuple4Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep, E : Rep>(_ f:(Ret.A, Ret.B)->(A, B, C, D, E)) -> QueryImpl<DS, Tuple5Rep<A, B, C, D, E>> {
        return _map { ret in
            Tuple5Rep(tuple: ret.wrapped |> f)
        }
    }
}

public extension QueryLike where Ret : Tuple3RepProtocol, Ret.Tuple == Tuple3<Ret.A, Ret.B, Ret.C> {
    public func map<BRet : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->BRet) -> QueryImpl<DS, BRet> {
        return _map { ret in
            ret.wrapped |> f
        }
    }
    
    public func filter(_ f:(Ret.A, Ret.B, Ret.C)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.wrapped |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return _map { ret in
            Tuple2Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return _map { ret in
            Tuple3Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return _map { ret in
            Tuple4Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep, E : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->(A, B, C, D, E)) -> QueryImpl<DS, Tuple5Rep<A, B, C, D, E>> {
        return _map { ret in
            Tuple5Rep(tuple: ret.wrapped |> f)
        }
    }
}

public extension QueryLike where Ret : Tuple4RepProtocol, Ret.Tuple == Tuple4<Ret.A, Ret.B, Ret.C, Ret.D> {
    public func map<BRet : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->BRet) -> QueryImpl<DS, BRet> {
        return _map { ret in
            ret.wrapped |> f
        }
    }
    
    public func filter(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.wrapped |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return _map { ret in
            Tuple2Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return _map { ret in
            Tuple3Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return _map { ret in
            Tuple4Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep, E : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->(A, B, C, D, E)) -> QueryImpl<DS, Tuple5Rep<A, B, C, D, E>> {
        return _map { ret in
            Tuple5Rep(tuple: ret.wrapped |> f)
        }
    }
}

public extension QueryLike where Ret : Tuple5RepProtocol, Ret.Tuple == Tuple5<Ret.A, Ret.B, Ret.C, Ret.D, Ret.E> {
    public func map<BRet : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D, Ret.E)->BRet) -> QueryImpl<DS, BRet> {
        return _map { ret in
            ret.wrapped |> f
        }
    }
    
    public func filter(_ f:(Ret.A, Ret.B, Ret.C, Ret.D, Ret.E)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.wrapped |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D, Ret.E)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return _map { ret in
            Tuple2Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D, Ret.E)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return _map { ret in
            Tuple3Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D, Ret.E)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return _map { ret in
            Tuple4Rep(tuple: ret.wrapped |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep, E : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D, Ret.E)->(A, B, C, D, E)) -> QueryImpl<DS, Tuple5Rep<A, B, C, D, E>> {
        return _map { ret in
            Tuple5Rep(tuple: ret.wrapped |> f)
        }
    }
}

////////////////////////////////////////////////////// FILTER //////////////////////////////////////////////////////

public extension QueryLike {
    public func filter(_ f: (Ret)->Predicate) -> QueryImpl<DS, Ret> {
        return query.filter(f)
    }
}

public extension Query {
    public func filter(_ f: (Ret)->Predicate) -> QueryImpl<DS, Ret> {
        return QueryImpl(dataset: dataset, ret: ret, predicate: f(ret) && predicate, order: order, limit: limit)
    }
}

////////////////////////////////////////////////////// TAKE/DROP //////////////////////////////////////////////////////

public extension QueryLike {
    public func take(_ n:Int, drop:Int? = nil) -> QueryImpl<DS, Ret> {
        return query.take(n, drop: drop)
    }
}

public extension Query {
    public func take(_ n:Int, drop:Int? = nil) -> QueryImpl<DS, Ret> {
        let limit = Limit(limit: n, offset: drop)
        return QueryImpl(dataset: dataset, ret: ret, predicate: predicate, order: order, limit: limit)
    }
}
