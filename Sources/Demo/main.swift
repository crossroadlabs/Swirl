//===--- main.swift ------------------------------------------------------===//
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
import ExecutionContext
import Future

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

public protocol QueryLike {
    associatedtype DS : Dataset
    associatedtype Ret : Rep
    
    func map<BRet : Rep>(_ f:(Ret)->BRet) -> QueryImpl<DS, BRet>
    func filter(_ f:(Ret)->Predicate) -> QueryImpl<DS, Ret>
}

//Bound Query
public protocol Query : QueryLike {
    var dataset:DS {get}
    var ret:Ret {get}
    var predicate:Predicate {get}
    var order:Any {get}
    var limit:Limit? {get}
}

public extension Query {
    public func map<BRet : Rep>(_ f:(Ret)->BRet) -> QueryImpl<DS, BRet> {
        return QueryImpl(dataset: dataset, ret: f(ret), predicate: predicate, order: order, limit: limit)
    }
}

public protocol Renderable {
    func render(dialect:Dialect) -> SQL
}

private func next(name: String?) -> String {
    guard let name = name else {
        return "a"
    }
    
    let scalars = name.unicodeScalars
    let char = UnicodeScalar(scalars[scalars.startIndex].value + 1)!
    
    return String(describing: char)
}

private func name(at index: Int) -> String {
    let scalars = "a".unicodeScalars
    let char = UnicodeScalar(scalars[scalars.startIndex].value.advanced(by: index))!
    
    return String(describing: char)
}

public typealias Q = QueryImpl<ErasedTable, ErasedTable>

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

public extension QueryLike {
    /*public func map<A: Rep, B : Rep>(_ f:(Ret)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return self.map { ret in
            let t = f(ret)
            return Tuple2Rep<A, B>(value: t)
        }
    }*/
    
    /*public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return map { ret in
            Tuple3Rep(value: ret.tuple |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return map { ret in
            Tuple4Rep(value: ret.tuple |> f)
        }
    }*/
}

public extension QueryLike where Ret : Tuple2RepProtocol {
    public func map<BRet : Rep>(_ f:(Ret.A, Ret.B)->BRet) -> QueryImpl<DS, BRet> {
        return map { ret in
            ret.tuple |> f
        }
    }
    
    public func filter(_ f:(Ret.A, Ret.B)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.tuple |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A, Ret.B)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return map { ret in
            Tuple2Rep(value: ret.tuple |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return map { ret in
            Tuple3Rep(value: ret.tuple |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return map { ret in
            Tuple4Rep(value: ret.tuple |> f)
        }
    }
}

public extension QueryLike where Ret : Tuple3RepProtocol {
    public func map<BRet : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->BRet) -> QueryImpl<DS, BRet> {
        return map { ret in
            ret.tuple |> f
        }
    }
    
    public func filter(_ f:(Ret.A, Ret.B, Ret.C)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.tuple |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return map { ret in
            Tuple2Rep(value: ret.tuple |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return map { ret in
            Tuple3Rep(value: ret.tuple |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B, Ret.C)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return map { ret in
            Tuple4Rep(value: ret.tuple |> f)
        }
    }
}

public extension QueryLike where Ret : Tuple4RepProtocol {
    public func map<BRet : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->BRet) -> QueryImpl<DS, BRet> {
        return map { ret in
            ret.tuple |> f
        }
    }
    
    public func filter(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->Predicate) -> QueryImpl<DS, Ret> {
        return filter { ret in
            ret.tuple |> f
        }
    }
    
    public func map<A: Rep, B : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return map { ret in
            Tuple2Rep(value: ret.tuple |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->(A, B, C)) -> QueryImpl<DS, Tuple3Rep<A, B, C>> {
        return map { ret in
            Tuple3Rep(value: ret.tuple |> f)
        }
    }
    
    public func map<A: Rep, B : Rep, C : Rep, D : Rep>(_ f:(Ret.A, Ret.B, Ret.C, Ret.D)->(A, B, C, D)) -> QueryImpl<DS, Tuple4Rep<A, B, C, D>> {
        return map { ret in
            Tuple4Rep(value: ret.tuple |> f)
        }
    }
}

public extension QueryLike {
    public func map<A: Rep, B : Rep>(_ f:(Ret)->(A, B)) -> QueryImpl<DS, Tuple2Rep<A, B>> {
        return map { ret in
            Tuple2Rep(value: f(ret))
        }
    }
}

public extension Query {
    /*public func map<Z>(_ f: (Ret.Value)->Z) -> QueryImpl<DS, Z> {
        return QueryImpl(dataset: dataset, ret: f(ret), predicate: predicate, order: order, limit: limit)
    }*/
    
    /*public func map(_ f: (Ret)->Column) -> QueryImpl<DS, ErasedTable> {
        let names = [f(self.ret).name]
        return map(names)
    }
    
    public func map(_ f: (Ret)->[Column]) -> QueryImpl<DS, ErasedTable> {
        let names = f(self.ret).map{$0.name}
        return map(names)
    }
    
    public func map(_ f: @autoclosure (Ret)->[String]) -> QueryImpl<DS, ErasedTable> {
        let ret = ErasedTable(name: self.dataset.name, columns: .list(f(self.ret)))
        return QueryImpl(dataset: dataset,
                         ret: ret,
                         predicate: predicate,
                         order: order,
                         limit: limit)
    }
    
    public func map(_ f: @autoclosure (Ret)->String) -> QueryImpl<DS, ErasedTable> {
        let ret = ErasedTable(name: self.dataset.name, columns: .list([f(self.ret)]))
        return QueryImpl(dataset: dataset,
                         ret: ret,
                         predicate: predicate,
                         order: order,
                         limit: limit)
    }*/
}

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

public extension Table where Self : Rep {
    //cross join
    public func zip<B : Table>(with table: B) -> QueryImpl<Join<Self, B>, Tuple2Rep<Self, B>> where B : Rep {
        let join:Join<Self, B> = .cross(left: self, right: table)
        //TODO: glue predicated with AND
        return QueryImpl(dataset: join, ret: Tuple2Rep(self, table))
    }
    
    //inner join
    public func zip<B : Table>(with table: B, _ condition: JoinCondition) -> QueryImpl<Join<Self, B>, Tuple2Rep<Self, B>> where B : Rep {
        let join:Join<Self, B> = .inner(left: self, right: table, condition: condition)
        //TODO: glue predicated with AND
        return QueryImpl(dataset: join, ret: Tuple2Rep(self, table))
    }
    
    //outer join
    public func zip<B : Table>(with table: B, outer direction: JoinDirection, _ condition: JoinCondition) -> QueryImpl<Join<Self, B>, Tuple2Rep<Self, B>> where B : Rep {
        let join:Join<Self, B> = .outer(left: self, right: table, condition: condition, direction: direction)
        //TODO: glue predicated with AND
        return QueryImpl(dataset: join, ret: Tuple2Rep(self, table))
    }
}

//TODO: implement for Joins
public extension Table where Self : Rep {
    public func zip<B : Table>(with table:B, _ f: (Self, B) -> Predicate) -> QueryImpl<Join<Self, B>, Tuple2Rep<Self, B>> where B : Rep {
        return zip(with: table, .on(f(self, table)))
    }
    
    public func zip<B : Table>(with table:B, outer direction: JoinDirection, _ f: (Self, B) -> Predicate) -> QueryImpl<Join<Self, B>, Tuple2Rep<Self, B>> where B : Rep {
        return zip(with: table, outer: direction, .on(f(self, table)))
    }
}

public extension Query {
    public func filter(_ f: (Ret)->Predicate) -> QueryImpl<DS, Ret> {
        return QueryImpl(dataset: dataset, ret: ret, predicate: f(ret) && predicate, order: order, limit: limit)
    }
}

/*public extension Query where DS : JoinProtocol, DS.Left : Table {
    public func map(_ f: (DS.Left, DS.Right)->[Column]) -> QueryImpl<Join<DS.Left, DS.Right>> {
        var colmap = [String: [String]]()
        
        let datasets = dataset.datasets
        let (left, right) = datasets
        
        let cols = datasets |> f
        for col in cols {
            let name = col.table.name
            if nil == colmap[name] {
                colmap[name] = [String]()
            }
            colmap[name]!.append(col.name)
        }
        
        let newleft = DS.Left(name: left.name, columns: .list(colmap[left.name] ?? []))
        let newright = DS.Right(name: right.name, columns: .list(colmap[right.name] ?? []))
        
        let join = dataset.replace(left: newleft, right: newright)
        
        return QueryImpl(dataset: join,
                         predicate: predicate,
                         order: order,
                         limit: limit)
    }
}*/

public extension Query {
    public func take(_ n:Int, drop:Int? = nil) -> QueryImpl<DS, Ret> {
        let limit = Limit(limit: n, offset: drop)
        return QueryImpl(dataset: dataset, ret: ret, predicate: predicate, order: order, limit: limit)
    }
}

class SQLiteDialect : Dialect {
    var proto:String {
        return "sqlite"
    }
}

extension Dialect {
    var param:String {
        return "?"
    }
    
    func render(column: String, table: String, escape:Bool) -> SQL {
        let col = escape ? "`\(column)`" : column
        return SQL(query: "\(table).\(col)", parameters: [])
    }
    
    func render(columns: Columns, table: String) -> [SQL] {
        switch columns {
        case .all:
            return [render(column: "*", table: table, escape: false)]
        case .list(let columns):
            return columns.map {render(column: $0, table: table, escape: true)}
        }
    }
    
    func render(rep:ErasedRep, aliases:[String: String]) -> SQL {
        return rep.render(dialect: self, aliases: aliases)
    }
    
    func render<Ret : Rep>(columns ret:Ret, aliases:[String: String]) -> SQL {
        return ret.stripe.map {($0, aliases)}.map(render).joined(separator: ", ")
    }
    
    func render(limit:Limit) -> SQL {
        switch limit {
        case .limit(let limit):
            return SQL(query: "LIMIT \(limit)", parameters: [])
        case .offset(let offset, limit: let limit):
            return SQL(query: "LIMIT \(limit) OFFSET \(offset)", parameters: [])
        }
    }
    
    func render<DS: Dataset, Ret : Rep>(dataset:DS, ret: Ret, filter:Predicate, limit:Limit?) -> SQL {
        let tables = dataset.tables
        let aliases = toMap(tables.reversed().enumerated().map { (i, table) in
            (table.name, name(at: i))
        })
        
        let columns = render(columns: ret, aliases: aliases)
        let source = dataset.render(dialect: self, aliases: aliases)
        
        let base = SQL(query: "SELECT", parameters: [])
        let ssql:SQL? = columns + " FROM " + source
        let fsql = filter.render(dialect: self, aliases: aliases).map {"WHERE " + $0}
        let lsql = limit.map(render)
        
        let sql = [ssql, fsql, lsql].flatMap {$0}.reduce(base) { z, a in
            z + " " + a
        }
        
        let paramsFixed = sql.parameters.map { param -> Any? in
            param.map { value in
                switch value {
                //SQLITE doesn't support boolean
                case let b as Bool:
                    return b ? 1 : 0
                default:
                    return value
                }
            }
        }
        
        return SQL(query: sql.query, parameters: paramsFixed)
    }
    
    func render(table:Table, aliases: [String: String]) -> SQL {
        let alias = aliases[table.name]!
        return SQL(query: "`\(table.name)` as \(alias)", parameters: [])
    }
    
    func render<T>(value: T?) -> SQL {
        return SQL(query: param, parameters: [value])
    }
    
    func render(op:BinaryOp, _ a:SQL, _ b:SQL) -> SQL {
        switch op {
        case .and:
            return "(" + a + " AND " + b + ")"
        case .or:
            return "(" + a + " OR " + b + ")"
        case .xor:
            return "(" + a + " IS NOT " + b + ")"
        case .eq:
            return "(" + a + " IS " + b + ")"
        case .gt:
            return "(" + a + " > " + b + ")"
        case .lt:
            return "(" + a + " < " + b + ")"
        case .gte:
            return "(" + a + " >= " + b + ")"
        case .lte:
            return "(" + a + " <= " + b + ")"
        case .like:
            return "(" + a + " LIKE " + b + ")"
        default:
            fatalError("Not implemented")
        }
    }
    
    func render(metaValue:MetaValueProtocol?, aliases: [String: String]) -> SQL {
        return metaValue.map { value in
            value.render(dialect: self, aliases: aliases)
        } ?? self.render(value: Optional<Any>.none)
    }
    
    func render(comparison op: BinaryOp, _ a:ErasedRep, _ b:ErasedRep, aliases: [String: String]) -> SQL {
        return render(op: op, render(rep: a, aliases: aliases), render(rep: b, aliases: aliases))
    }
    
    func render(compound op: BinaryOp, _ a:Predicate, _ b:Predicate, aliases: [String: String]) -> SQL? {
        let asql = a.render(dialect: self, aliases: aliases) ?? render(value: Optional<Any>.none)
        let bsql = b.render(dialect: self, aliases: aliases) ?? render(value: Optional<Any>.none)
        
        return render(op: op, asql, bsql)
    }
    
    /*return dialect.render(value: bool)
    case .comparison(op: let op, a: let a, b: let b):
    return dialect.render(comparison: op, a, b)
    case .compound(op: let op, let a, let b):
    return dialect.render(compound: op, a, b)*/
    
    func render(direction:JoinDirection) -> String {
        switch direction {
        case .left:
            return "LEFT"
        case .right:
            return "RIGHT"
        case .full:
            return "FULL"
        }
    }
    
    func render<J: JoinProtocol>(join:J, aliases: [String: String]) -> SQL {
        switch join.join {
            //CROSS
        case .cross(left: let left, right: let right):
            return left.render(dialect: self, aliases: aliases) + " CROSS JOIN " + right.render(dialect: self, aliases: aliases)
            //NATURAL
        case .inner(left: let left, right: let right, condition: .natural):
            return left.render(dialect: self, aliases: aliases) + " NATURAL JOIN " + right.render(dialect: self, aliases: aliases)
            //INNER with USING
        case .inner(left: let left, right: let right, condition: .using(let columns)):
            let using = columns.map {"`\($0)`"}.joined(separator: ", ")
            return left.render(dialect: self, aliases: aliases) + " INNER JOIN " + right.render(dialect: self, aliases: aliases) + " USING(\(using))"
            //INNER with ON
        case .inner(left: let left, right: let right, condition: .on(let predicate)):
            let on = predicate.render(dialect: self, aliases: aliases) ?? render(value: true)
            return left.render(dialect: self, aliases: aliases) + " INNER JOIN " + right.render(dialect: self, aliases: aliases) + " ON " + on
            //OUTER with USING
        case .outer(left: let left, right: let right, condition: .using(let columns), direction: let _direction):
            let direction = render(direction: _direction)
            let using = columns.map {"`\($0)`"}.joined(separator: ", ")
            return left.render(dialect: self, aliases: aliases) + " \(direction) OUTER JOIN " + right.render(dialect: self, aliases: aliases) + " USING(\(using))"
            //OUTER with ON
        case .outer(left: let left, right: let right, condition: .on(let predicate), direction: let _direction):
            let direction = render(direction: _direction)
            let on = predicate.render(dialect: self, aliases: aliases) ?? render(value: true)
            return left.render(dialect: self, aliases: aliases) + " \(direction) OUTER JOIN " + right.render(dialect: self, aliases: aliases) + " ON " + on
        default:
            fatalError("Not implemented")
        }
    }
}

public protocol MetaValueProtocol {
    func render(dialect:Dialect, aliases:[String: String]) -> SQL
}

public enum MetaValue<T> {
    case column(Column)
    case `static`(T)
}

extension MetaValue : MetaValueProtocol {
}

extension Null {
    var meta:MetaValueProtocol? {
        return nil
    }
}

extension Predicate {
    func render(dialect:Dialect, aliases:[String: String]) -> SQL? {
        switch self {
        case .null:
            return nil
        case .bool(let bool):
            return dialect.render(value: bool)
        case .comparison(op: let op, a: let a, b: let b):
            return dialect.render(comparison: op, a, b, aliases: aliases)
        case .compound(op: let op, let a, let b):
            return dialect.render(compound: op, a, b, aliases: aliases)
        }
    }
}

public extension MetaValue {
    public func render(dialect:Dialect, aliases:[String: String]) -> SQL {
        switch self {
        case .column(let column):
            let alias = aliases[column.table.name]!
            return dialect.render(column: column.name, table: alias, escape: true)
        case .static(let value):
            return dialect.render(value: value)
        }
    }
}

let t = ErasedTable(name: "lala")
let c = ErasedColumn(name: "qwe", in: t)

//let p:Predicate = 1 <= 2 || 2 > c//(c == "b" && "b" != c || c != c && 1 == c) != (c == "a")
//print(p)

//let driver = SQLiteDriver()
//let connection = try driver.connect(url: "sqlite:///tmp/crlrsdc3.sqlite", params: [:])

//
/*try connection.execute(query: "CREATE TABLE person(id INTEGER PRIMARY KEY AUTOINCREMENT, firstname TEXT, lastname TEXT);", parameters: [], named: [:])
try connection.execute(query: "INSERT INTO person(firstname, lastname) VALUES(?, :last);", parameters: ["Daniel",], named: [":last":"Leping"])
try connection.execute(query: "INSERT INTO person(firstname, lastname) VALUES(?, ?);", parameters: ["John", "Lennon"], named: [:])
try connection.execute(query: "INSERT INTO person(firstname, lastname) VALUES(@first, :last);", parameters: [], named: [":last":"McCartney", "@first": "Paul"])
try connection.execute(query: "INSERT INTO person(firstname, lastname) VALUES(@first, :last);", parameters: [], named: [":last":"Trump", "@first": "Donald"])
try connection.execute(query: "INSERT INTO person(firstname) VALUES(@first);", parameters: [], named: ["@first": "Sky"])

try connection.execute(query: "CREATE TABLE comment(id INTEGER PRIMARY KEY AUTOINCREMENT, person_id INTEGER, comment TEXT, FOREIGN KEY(person_id) REFERENCES person(id));", parameters: [], named: [:])
try connection.execute(query: "INSERT INTO comment(person_id, comment) VALUES(?, ?);", parameters: [1, "Awesome"], named: [:])
try connection.execute(query: "INSERT INTO comment(person_id, comment) VALUES(?, ?);", parameters: [2, "Cool"], named: [:])
try connection.execute(query: "INSERT INTO comment(person_id, comment) VALUES(?, ?);", parameters: [3, "Star"], named: [:])*/

//try connection.execute(query: "INSERT INTO comment(person_id, comment) VALUES(?, ?);", parameters: [1, "Developer"], named: [:])
//try connection.execute(query: "INSERT INTO comment(person_id, comment) VALUES(?, ?);", parameters: [2, "Musician"], named: [:])
//try connection.execute(query: "INSERT INTO comment(person_id, comment) VALUES(?, ?);", parameters: [3, "Musician"], named: [:])

//let res = try connection.execute(query: "SELECT ? FROM test1 as a;", parameters: ["*"/*, "test1", "a"*/], named: [:])

/*guard let results = res else {
    print("No results arrived...")
    exit(1)
}

print("Coulumns:", try results.columns())
while let row = try results.next() {
    print("Row:", row)
}

print("OK")*/

print("aa:", next(name: "a"))
print(next(name: "b"))

let manager = SwirlManager()
manager.register(driver: SQLiteDriver())

let swirl = try manager.swirl(url: "sqlite:///tmp/crlrsdc3.sqlite")

//let t1 = pool.select(from: "test1").map{ $0["firstname"] }
//let t2 = pool.select(from: "test2").map("lastname")

let person = Q.table(name: "person")
let comment = Q.table(name: "comment")

//pool.select(from: "test1").map {t1 in [t1["firstname"]]}.zip(with: t2, .using(["id"]), type: .left)
// SELECT a.`id`, a.`name` from `test1` as a;

// SELECT a.`firstname`, b.`comment` from `test1` as a INNER JOIN `test2` as b USING('id') WHERE a.`firstname` == "Daniel";

person.map { p in
    (p.c("id", type: Int.self), p["firstname"].bind(String.self))
}.filter { id, name in
    id < 3 && name ~= "%oh%"
}.take(2).execute(in: swirl).flatMap{$0}.flatMap { results in
    results.columns.zip(results.all())
}.onSuccess { (cols, rows) in
    print(cols)
    for row in rows {
        //print(row)
        print(row.flatMap{$0})
    }
}.onFailure { e in
    print("!!!Error:", e)
}

/*person.zip(with: comment) { p, c in
    p["id"] == c["person_id"]
}.map { p, c in
    (p["id"], p["firstname"], p["lastname"], c["comment"])
}.filter { id, _, _, _ in
    id > 2
}.map { id, first, last, comment in
    (first, last, comment)
}.filter { id, first, last, comment in
    comment == "Musician"
}.execute(in: swirl).flatMap{$0}.flatMap { results in
    results.columns.zip(results.all())
}.onSuccess { (cols, rows) in
    print(cols)
    for row in rows {
        //print(row)
        print(row.flatMap{$0})
    }
}.onFailure { e in
    print("!!!Error:", e)
}*/


//person.zip(with: comment, outer: .left) { person, comment in
//    person["id"] == comment["person_id"]
//}.map { p, c in
//    [p["firstname"], p["lastname"], c["comment"]]
//}/*.filter { t1, _ in
//    t1["firstname"] == "Daniel" || t1["lastname"] == "McCartney"
//}*/.filter { person, comment in
//    person["lastname"] ~= "%epi%"
//}/*.filter { person, comment in
//    comment["comment"] == "Musician"// || comment["comment"] == "Cool"
//}*/.take(1, drop: 1).execute(in: swirl).flatMap{$0}.flatMap { results in
//    results.columns.zip(results.all())
//}.onSuccess { (cols, rows) in
//    print(cols)
//    for row in rows {
//        print(row)
//        //print(row.flatMap{$0})
//    }
//}.onFailure { e in
//    print("!!!Error:", e)
//}

ExecutionContext.mainProc()
