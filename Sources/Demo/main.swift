
import Foundation
import ExecutionContext
import Future

import RDBC

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

rdbc.pool(url: "sqlite:///tmp/crlrsdc.sqlite")
    .execute(query: "SELECT * FROM test1;", parameters: [], named: [:]).flatMap{$0}
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
