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

class SQLiteDialect : Dialect {
    var proto:String {
        return "sqlite"
    }
}

extension Dialect {
    var param:String {
        return "?"
    }
    
    var phony:String {
        return "!"
    }
    
    func render(column: String, table: String, escape:Bool) -> SQL {
        let col = escape ? "`\(column)`" : column
        let sql = table == phony ? col : "\(table).\(col)"
        return SQL(query: sql, parameters: [])
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
    
    func aliases<DS : Dataset>(dataset:DS) -> [String: String] {
        let tables = dataset.tables
        return toMap(tables.reversed().enumerated().map { (i, table) in
            (table.name, name(at: i))
        })
    }
    
    func phonyAliases<DS : Dataset>(dataset:DS) -> [String: String] {
        return toMap(aliases(dataset: dataset).map { k, _ in
            (k, phony)
        })
    }
    
    func render(values row: [ErasedRep], aliases: [String: String]) -> SQL {
        return "(" + row.map {($0, aliases)}.map(render).joined(separator: ", ") + ")"
    }
    
    func render(values rows: [[ErasedRep]], aliases: [String: String]) -> SQL {
        return rows.map {($0, aliases)}.map(render).joined(separator: ", \n\t")
    }
    
    func render<DS: TableProtocol, Ret: Rep>(insert vsql: SQL, to table:DS, ret: Ret) -> SQL {
        //yes aliases must be empty
        let tsql = table.render(dialect: self, aliases: [:])
        
        let phony = phonyAliases(dataset: table)
        
        let csql = render(columns: ret, aliases: phony)
        
        return "INSERT INTO " + tsql + " (" + csql + ") VALUES \n\t" + vsql
    }
    
    func render<DS: TableProtocol, Ret: Rep>(insert row: [ErasedRep], to table:DS, ret: Ret) -> SQL {
        let vsql = render(values: row, aliases: [:])
        return render(insert: vsql, to: table, ret: ret)
    }
    
    func render<DS: TableProtocol, Ret: Rep>(insert rows: [[ErasedRep]], to table:DS, ret: Ret) -> SQL {
        let vsql = render(values: rows, aliases: [:])
        return render(insert: vsql, to: table, ret: ret)
    }
    
    func render<DS: Dataset, Ret : Rep>(dataset:DS, ret: Ret, filter:Predicate, limit:Limit?) -> SQL {
        let aliases = self.aliases(dataset: dataset)
        
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
        let alias = aliases[table.name]
        let text = alias.map { alias in
            "`\(table.name)` as \(alias)"
        } ?? "`\(table.name)`"
        
        return SQL(query: text, parameters: [])
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
        case .neq:
            return "(" + a + " IS NOT " + b + ")"
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
        }
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

//pool.select(from: "test1").map {t1 in [t1["firstname"]]}.zip(with: t2, .using(["id"]), type: .left)
// SELECT a.`id`, a.`name` from `test1` as a;

// SELECT a.`firstname`, b.`comment` from `test1` as a INNER JOIN `test2` as b USING('id') WHERE a.`firstname` == "Daniel";

let person = Q.table(name: "person")
let comment = Q.table(name: "comment")

struct Comment {
    let id:Int
    let comment:String
    
    init(id:Int, comment:String) {
        self.id = id
        self.comment = comment
    }
}

extension Comment : Entity {
    typealias Tuple = Tuple2<Int, String>
    
    init(tuple: (Int, String)) {
        self.init(id: tuple.0, comment: tuple.1)
    }
    
    var tuple:(Int, String) {
        return (id, comment)
    }
}

//class Comments : TypedTable<Tuple2<Int, String>>, QueryLike {
class Comments : TypedTable<Comment>, QueryLike {
    public typealias DS = Comments
    public typealias Ret = Comments
    
    override class var table: String {
        return "comment"
    }
    
    let id: TypedColumn<Int> = Comments.column("id")
    let personId: TypedColumn<Int> = Comments.column("person_id")
    let comment: TypedColumn<String> = Comments.column("comment")
    
    init() {
        super.init(all: (id, comment))
    }
}
let comments = Comments()

/*comments.zip(with: person) { c, p in
    c.personId == p["id"].bind(Int.self)
}.filter { c, p in
    c.id > 3
}.map { c, p in
    (p["firstname"].bind(String.self), c.comment)
}.result.execute(in: swirl).onSuccess { join in
    for (name, comment) in join {
        print("\(name) is \(comment)")
    }
}

comments.map { c in
    (c.id, c.comment)
}.filter { id, _ in
    id < 3 || id > 5
}.result.execute(in: swirl).onSuccess { comments in
        //every row is a tuple, types are preserved
    for (id, comment) in comments {
        print("'\(comment)' identified with ID: \(id)")
    }
}*/

[comments.map {($0.personId, $0.comment)} += (5, "WTF1"),
 comments.map {($0.personId, $0.comment)} += (5, "WTF"),
 comments.map {($0.personId, $0.comment)} += (5, "WTF3")].execute(in: swirl).onSuccess { ressult in
    print("Inserted shit:", ressult)
}

swirl.execute(comments.map {($0.personId, $0.comment)} += [(5, "WTF1"),
                                                           (5, "WTF"),
                                                           (5, "WTF3")]).onSuccess { ressult in
                print("Inserted shit:", ressult)
}.onFailure { e in
    print(e)
}

/*swirl.execute(operation: comments += Comment(id: 1257, comment: "Test222")).onSuccess { res in
    print("Inserted shit:", res)
}.onFailure { e in
    print("E!!:", e)
}*/

/*swirl.execute(operation: comments.map {($0.personId, $0.comment)} += (5, "Test222")).onSuccess { res in
    print("Inserted shit:", res)
    }.onFailure { e in
        print("E!!:", e)
}*/

/*comments.filter { comment in
    comment.id < 3 || comment.id > 5
}.result.execute(in: swirl).onSuccess { comments in
    //every row is a tuple, types are preserved
    for comment in comments {
        print("'\(comment.comment)' identified with ID: \(comment.id)")
    }
//    for (id, comment) in comments {
//        print("'\(comment)' identified with ID: \(id)")
//    }
}*/

/*person.map { p in
    //
    (p.c("id", type: Int.self), p["firstname"].bind(String.self))
}.filter { id, name in
    //everything is typesafe, e.g. id == “some” gives compile time error
    id > 1 && name ~= "%oh%"
}.take(2)
.result.execute(in: swirl).onSuccess { rows in
    //every row is a tuple, types are preserved
    for (id, name) in rows {
        print("\(name) identified with ID: \(id)")
    }
}*/

/*person.map { p in
    (p.c("id", type: Int.self), p["firstname"].bind(String.self))
}.filter { id, name in
    id > 1 && name ~= "%oh%"
}.take(2).result.execute(in: swirl).onSuccess { rows in
    for (id, name) in rows {
        print("\(name) identified with ID: \(id)")
    }
}.onFailure { e in
    print("!!!Error:", e)
}*/

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
