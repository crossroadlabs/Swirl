
import Foundation

import Boilerplate
import ExecutionContext
import Future

import RDBC

//Bound Query
public protocol Query {
    associatedtype DS : Dataset
    
    var connection:Connection {get}
    
    var dataset:DS {get}
    var predicate:Predicate {get}
    var order:Any {get}
}

public protocol Renderable {
    func render(dialect:Dialect) -> SQL
}

public struct SQL {
    let query:String
    let parameters:[Any?]
    
    init(query:String, parameters:[Any?]) {
        self.query = query
        self.parameters = parameters
    }
}

extension Connection {
    func execute(sql:SQL) -> Future<ResultSet?> {
        return execute(query: sql.query, parameters: sql.parameters, named: [:])
    }
}

public extension Query where DS : Renderable {
    private func render(dialect:Dialect? = nil) throws -> SQL {
        guard let dialect = dialect.or(else: (connection as? DialectRich).map({$0.dialect})) else {
            throw RDBCFrameworkError.noDialect
        }
        
        fatalError()
    }
    
    public func execute(dialect:Dialect? = nil) -> Future<ResultSet?> {
        let connection = self.connection
        
        return future(context: immediate) {
            try self.render(dialect: dialect)
        }.flatMap { sql in
            connection.execute(sql: sql)
        }
    }
}

public struct QueryImpl<DS : Dataset> : Query {
    public let connection:Connection
    
    public let dataset:DS
    public let predicate:Predicate
    public let order:Any
    
    init(connection:Connection, dataset:DS, predicate:Predicate, order:Any) {
        self.connection = connection
        self.dataset = dataset
        self.predicate = predicate
        self.order = order
    }
}

public extension Query where DS : Table {
    public func map(_ f: (DS)->Column) -> QueryImpl<ErasedTable> {
        let names = [f(self.dataset).name]
        return map(names)
    }
    
    public func map(_ f: (DS)->[Column]) -> QueryImpl<ErasedTable> {
        let names = f(self.dataset).map{$0.name}
        return map(names)
    }
    
    public func map(_ f: @autoclosure (DS)->[String]) -> QueryImpl<ErasedTable> {
        let dataset = ErasedTable(name: self.dataset.name, columns: .list(f(self.dataset)))
        return QueryImpl(connection: connection,
                         dataset: dataset,
                         predicate: predicate,
                         order: order)
    }
    
    public func map(_ f: @autoclosure (DS)->String) -> QueryImpl<ErasedTable> {
        let dataset = ErasedTable(name: self.dataset.name, columns: .list([f(self.dataset)]))
        return QueryImpl(connection: connection,
                         dataset: dataset,
                         predicate: predicate,
                         order: order)
    }
}

public extension Connection {
    public func select(_ columns: [String]? = nil, from: String) -> QueryImpl<ErasedTable> {
        let columns:Columns = columns.map { seq in
            .list(seq)
        }.getOr(else: .all)
        
        return QueryImpl(connection: self,
                         dataset: ErasedTable(name: from, columns: columns),
                         predicate: .null,
                         order: "")
    }
}

public enum Columns {
    case list([String])
    case all
}

public protocol Dataset {
}

public protocol Table : Named, Dataset {
    var columns:Columns {get}
    
    init(name:String, columns:Columns)
    
    subscript(_ column:String) -> Column {get}
}

public extension Table where Self : Renderable {
    public func render(dialect:Dialect) -> SQL {
        fatalError("Not implemented")
    }
}

public protocol Column : Named {
    var table:Table {get}
}

public struct ErasedColumn : Column {
    public let name:String
    public let table:Table
    
    init(name: String, in table: Table) {
        self.name = name
        self.table = table
    }
}

public struct ErasedTable : Table, Renderable {
    public let name:String
    public let columns:Columns
    
    public init(name:String, columns:Columns = .all) {
        self.name = name
        self.columns = columns
    }
    
    public subscript(_ column:String) -> Column {
        return ErasedColumn(name: column, in: self)
    }
}

extension ErasedTable {
    init(_ table:Table) {
        self.init(name: table.name, columns: table.columns)
    }
}

public enum Predicate {
    case `true`
    case `false`
    case null
}

public enum JoinDirection {
    case left
    case right
    case full
}

public enum JoinCondition {
    case on(Predicate)
    case using([String])
    case natural
}

extension JoinCondition : ExpressibleByArrayLiteral {
    public typealias Element = String
    
    /// Creates an instance initialized with the given elements.
    public init(arrayLiteral elements: String...) {
        self = .using(elements)
    }
}

public protocol JoinProtocol : Dataset {
    associatedtype Left : Dataset
    associatedtype Right : Table
    
    var join:Join<Left, Right> {get}
}

public extension JoinProtocol where Self : Renderable {
    public func render(dialect:Dialect) -> SQL {
        fatalError("Not implemented")
    }
}

public extension JoinProtocol {
    public var datasets:(Left, Right) {
        switch join {
        case .cross(left: let left, right: let right):
            return (left, right)
        case .inner(left: let left, right: let right, condition: _):
            return (left, right)
        case .outer(left: let left, right: let right, condition: _, direction: _):
            return (left, right)
        }
    }
    
    func replace(left _left:Left? = nil, right _right:Right? = nil) -> Join<Left, Right> {
        switch join {
        case .cross(left: let left, right: let right):
            return .cross(left: _left ?? left, right: _right ?? right)
        case .inner(left: let left, right: let right, condition: let condition):
            return .inner(left: _left ?? left, right: _right ?? right, condition: condition)
        case .outer(left: let left, right: let right, condition: let condition, direction: let direction):
            return .outer(left: _left ?? left, right: _right ?? right, condition: condition, direction: direction)
        }
    }
}

public indirect enum Join<A : Dataset, B : Table> : JoinProtocol, Renderable {
    public typealias Left = A
    public typealias Right = B

    case cross(left:Left, right:Right)
    case inner(left:Left, right:Right, condition:JoinCondition)
    case outer(left:Left, right:Right, condition:JoinCondition, direction:JoinDirection)
    
    public var join:Join {
        return self
    }
}

public extension Query {
    //cross join
    public func zip<B : Table, Q : Query>(with query:Q) -> QueryImpl<Join<DS, B>> where Q.DS == B {
        let join:Join<DS, B> = .cross(left: dataset, right: query.dataset)
        //TODO: glue predicated with AND
        return QueryImpl(connection: connection, dataset: join, predicate: predicate, order: order)
    }
    
    public func zip<B : Table, Q : Query>(with query:Q, using condition: JoinCondition) -> QueryImpl<Join<DS, B>> where Q.DS == B {
        let join:Join<DS, B> = .inner(left: dataset, right: query.dataset, condition: condition)
        //TODO: glue predicated with AND
        return QueryImpl(connection: connection, dataset: join, predicate: predicate, order: order)
    }
    
    public func zip<B : Table, Q : Query>(with query:Q, using condition: JoinCondition, type direction: JoinDirection) -> QueryImpl<Join<DS, B>> where Q.DS == B {
        let join:Join<DS, B> = .outer(left: dataset, right: query.dataset, condition: condition, direction: direction)
        //TODO: glue predicated with AND
        return QueryImpl(connection: connection, dataset: join, predicate: predicate, order: order)
    }
}

public extension Query where DS : JoinProtocol, DS.Left : Table {
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
        
        return QueryImpl(connection: connection,
                         dataset: join,
                         predicate: predicate,
                         order: order)
    }
}

class SQLiteDialect : Dialect {
}

let rdbc = RDBC()
rdbc.register(driver: SQLiteDriver(), dialect: SQLiteDialect())

let pool = try rdbc.pool(url: "sqlite:///tmp/crlrsdc.sqlite")

//let t1 = pool.select(from: "test1").map{ $0["firstname"] }
//let t2 = pool.select(from: "test2").map("lastname")

/*pool.select(from: "").execute().flatMap{$0}.flatMap { results in
    results.columns.zip(results.all())
}.onSuccess { (cols, rows) in
    print(cols)
    for row in rows {
        print(row)
    }
}.onFailure { e in
    print("!!!Error:", e)
}*/

pool.execute(query: "SELECT * FROM test1;", parameters: [], named: [:]).flatMap{$0}
    .flatMap { results in
        results.columns.zip(results.all())
    }.onSuccess { (cols, rows) in
        print(cols)
        for row in rows {
            print(row)
        }
    }.onFailure { e in
        print("!!!Error:", e)
}

/*t1.zip(with: t2, using: ["id"]).map { t1, t2 in
    [t1["a"], t2["b"]]
}*/

//t1.join(t2) { t1, t2 in t1["id"] == t2["id"] }.filter { t1, _ in t1["firstname"] == "Daniel" || t2["lastname"] == "Lennon" }

/*public indirect enum Dataset {
    case table(Table)
    case join(Join)
}*/

/*public protocol Dialect {
    func select(connection:Connection, from: String) -> Query
}

public protocol Column : Named {
    //var type:Any.Type {get}
}

public enum Columns {
    case all
    case columns([Column])
}

public protocol Meta {
    var columns:Columns {get}
    
    subscript(_ name:String) -> Column {get}
}

public struct GenericColumn : Column {
    public let name:String
    
    public init(name:String) {
        self.name = name
    }
}

public struct GenericMeta : Meta {
    public static let identity:GenericMeta = GenericMeta()
    
    public let columns: Columns = .all
    
    public subscript(_ name:String) -> Column {
        return GenericColumn(name: name)
    }
}

func toDict<A : Hashable, B, S : Sequence>(_ s:S) -> [A: B] where S.Iterator.Element == (A, B) {
    var dict = [A: B]()
    for (a, b) in s {
       dict[a] = b
    }
    return dict
}

public struct SelectedMeta : Meta {
    public let columns: Columns
    private let columnsMap: [String: Column]
    
    public init(columns: [Column]) {
        self.columns = .columns(columns)
        self.columnsMap = toDict(columns.map { ($0.name, $0) })
    }
    
    public subscript(_ name:String) -> Column {
        return columnsMap[name]!
    }
}

extension Columns {
    var meta:Meta {
        switch self {
        case .columns(let columns):
            return SelectedMeta(columns: columns)
        default:
            return GenericMeta()
        }
    }
}

public enum Dataset {
    case query
    case join(String)
    case table(String, meta:Meta)
}

extension Dataset {
    func map(_ f:(Meta)->Meta) -> Dataset {
        switch self {
        case .table(let table, meta: let meta):
            return .table(table, meta: f(meta))
        default:
            fatalError()
        }
    }
}

public struct Select {
    let dataset:Dataset
    
    private init(dataset:Dataset) {
        self.dataset = dataset
    }
    
    func map(_ f:(Meta)->Column) -> Select {
        return map {[f($0)]}
    }
    
    func map<S : Sequence>(_ f:(Meta)->S) -> Select where S.Iterator.Element == Column {
        return map {.columns(Array(f($0)))}
    }
    
    func map(_ f:(Meta)->Columns) -> Select {
        return Select(dataset: dataset.map {f($0).meta})
    }
}

extension Select : ExpressibleByStringLiteral {
    public init(stringLiteral value: String) {
        dataset = .table(value, meta: GenericMeta.identity)
    }
    
    public init(extendedGraphemeClusterLiteral value: String) {
        self.init(stringLiteral: value)
    }
    
    public init(unicodeScalarLiteral value: String) {
        self.init(stringLiteral: value)
    }
}

public extension Connection {
    public func select1(from: Select) -> Select {
        return from
    }
}

public enum BinaryOp {
    case and
    case or
    
    case eq
    case neq
    case like
}

extension BinaryOp {
    var q:String {
        switch self {
        case .and:
            return "AND"
        case .or:
            return "OR"
        case .eq:
            return "=="
        default:
            fatalError()
        }
    }
}

public enum UnaryOp {
    case not
}

public indirect enum Predicate {
    case null
    case compound(op:BinaryOp, Predicate, Predicate)
    case transformation(op:UnaryOp, field:String)
    case comparison(op:BinaryOp, field:String, value:Any?)
}

extension Predicate {
    var q:String? {
        switch self {
        case .null:
            return nil
        case .compound(op: let op, let p1, let p2):
            return p1.q.flatMap { p1 in
                p2.q.map { p2 in
                    "(\(p1) \(op.q) \(p2))"
                }
            }
        case .comparison(op: let op, field: let field, value: let value):
            return "(\(field) \(op.q) \"\(value!)\")"
        default:
            return nil
        }
    }
}

extension Predicate : ExpressibleByNilLiteral {
    /// Creates an instance initialized with `nil`.
    public init(nilLiteral: ()) {
        self = .null
    }
}

public struct Query {
    fileprivate let _connection:Connection
    public let query:String
    public let predicate:Predicate
    
    internal init(connection:Connection, query:String) {
        self.init(connection: connection, query: query, predicate: nil)
    }
    
    private init(connection:Connection, query:String, predicate:Predicate) {
        self._connection = connection
        self.query = query
        self.predicate = predicate
    }
    
    func `where`(_ predicate:Predicate) -> Query {
        return Query(connection: _connection, query: query, predicate: predicate)
    }
}

public extension Query {
    func execute() -> Future<ResultSet?> {
        let w = self.predicate.q.map {" WHERE " + $0} ?? ""
        let q = self.query + w
        
        return self._connection.execute(query: q, parameters: [], named: [:])
    }
}

public extension Connection {
    public func select(custom query: String) -> Query {
        return Query(connection: self, query: query)
    }
    
    public func select(_ columns:[String]? = nil, from: String) -> Query {
        let columns = columns.map { columns in
            columns.joined(separator: ", ")
        } ?? "*"
        
        return select(custom: "SELECT \(columns) FROM \(from)")
    }
}

func ==<T>(field:String, value:T?) -> Predicate {
    return .comparison(op: .eq, field: field, value: value)
}

func &&(p1:Predicate, p2:Predicate) -> Predicate {
    return .compound(op: .and, p1, p2)
}

func ||(p1:Predicate, p2:Predicate) -> Predicate {
    return .compound(op: .or, p1, p2)
}

//let driver = SQLiteDriver()
//let connection = try driver.connect(url: "sqlite:///tmp/crlrsdc.sqlite", params: [:])
/*try connection.execute(query: "CREATE TABLE test1(id INTEGER PRIMARY KEY AUTOINCREMENT, firstname, lastname);", parameters: [], named: [:])
try connection.execute(query: "INSERT INTO test1(firstname, lastname) VALUES(?, :last);", parameters: ["Daniel",], named: [":last":"Leping"])
try connection.execute(query: "INSERT INTO test1(firstname, lastname) VALUES(?, ?);", parameters: ["John", "Lennon"], named: [:])
try connection.execute(query: "INSERT INTO test1(firstname, lastname) VALUES(@first, :last);", parameters: [], named: [":last":"McCartney", "@first": "Paul"])*/

/*guard let results = try connection.execute(query: "SELECT * FROM test1;", parameters: [], named: [:]) else {
    print("No results arrived...")
    exit(1)
}

print("Coulumns:", try results.columns())
while let row = try results.next() {
    print("Row:", row)
}

print("OK")*/

let rdbc = RDBC()
rdbc.register(driver: SQLiteDriver())

let pool = rdbc.pool(url: "sqlite:///tmp/crlrsdc.sqlite")

pool.select1(from: "test1").map {$0["firstname"]}

pool.execute(query: "SELECT * FROM test1;", parameters: [], named: [:]).flatMap{$0}
.flatMap { results in
    results.columns.zip(results.all())
}.onSuccess { (cols, rows) in
    print(cols)
    for row in rows {
        print(row)
    }
}.onFailure { e in
    print("!!!Error:", e)
}

pool
    .select(["id", "firstname"], from: "test1")
    .where("firstname" == "Daniel" && "lastname" == "Leping" || "lastname" == "Lennon")
    .execute().flatMap{$0}
    .flatMap { results in
        results.columns.zip(results.all())
    }.onSuccess { (cols, rows) in
        print(cols)
        for row in rows {
            print(row)
        }
    }.onFailure { e in
        print("!!!Error:", e)
}*/

/*
 //this actually worked
 @discardableResult
func print(results:ResultSet) -> Future<ResultSet> {
    return results.next().flatMap {$0}.map {(results, $0)}.onSuccess { (results, row) in
        print(row)
    }.map{$0.0}.flatMap {print(results: $0)}
}

cp.connect(url: "sqlite:///tmp/crlrsdc.sqlite").flatMap { connection in
    connection.execute(query: "SELECT * FROM test1;", parameters: [], named: [:])
}.flatMap{ results -> ResultSet? in results}.flatMap { results in
    results.columns.onSuccess() { columns in
        print(columns)
    }.map {(results, $0)}
}.map{$0.0}.onSuccess { rs in
    print(results: rs)
}*/

//_ = printResults(aresults)

/*aresults.onSuccess { rs in
    rs.next().flatMap{$0}.map {(rs, $0)}.onSuccess { rs, row in
        print(row)
    }.map{$0.0}.fla
}*/



ExecutionContext.mainProc()
