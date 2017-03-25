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

import ExecutionContext

import Swirl
import SwirlSQLite

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

//let t = ErasedTable(name: "lala")
//let c = ErasedColumn(name: "qwe", in: t)

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

comments.zip(with: person) { c, p in
    c.personId == p["id"].bind(Int.self)
}.filter { c, p in
    c.id > 3
}.map { c, p in
    (p["firstname"].uppercased(), c.comment.lowercased())
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
}

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

person.map { p in
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
}.onFailure { e in
    print("EE", e)
}

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
