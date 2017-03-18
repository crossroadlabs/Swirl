
import Foundation
import ExecutionContext
import Future

import RDBC

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
    .where("firstname" == "Daniel" && "lastname" == "Leping")
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
}

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
