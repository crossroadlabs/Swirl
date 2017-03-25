//===--- SQLiteDialect.swift ------------------------------------------------------===//
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

import Swirl

import RDBCSQLite

public class SQLiteDriver : SyncSwirlDriver {
    public init() {
        try! super.init(driver: RDBCSQLite.SQLiteDriver(), dialect: SQLiteDialect())
    }
}

public class SQLiteDialect {
}

//front API
extension SQLiteDialect : Dialect {
    public var proto: String {
        return "sqlite"
    }
    
    public var affected: String {
        return "count"
    }
    
    //front API
    public func render<DS: Dataset, Ret : Rep>(select ret: Ret, from dataset:DS, filter:Predicate, limit:Limit?) -> SQL {
        let aliases = self.aliases(dataset: dataset)
        
        let columns = render(columns: ret, aliases: aliases)
        let source = dataset.render(renderer: self, aliases: aliases)
        
        let base = SQL(query: "SELECT", parameters: [])
        let ssql:SQL? = columns + " FROM " + source
        let fsql = filter.render(renderer: self, aliases: aliases).map {"WHERE " + $0}
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
    
    public func render<DS: TableProtocol, Ret: Rep>(insert row: [ErasedRep], into table:DS, ret: Ret) -> SQL {
        let vsql = render(values: row, aliases: [:])
        return render(insert: vsql, to: table, ret: ret)
    }
    
    public func render<DS: TableProtocol, Ret: Rep>(insert rows: [[ErasedRep]], into table:DS, ret: Ret) -> SQL {
        let vsql = render(values: rows, aliases: [:])
        return render(insert: vsql, to: table, ret: ret)
    }
}

//front API
extension SQLiteDialect : Renderer {
    //hooks for renderables
    public func render<T>(value: T?) -> SQL {
        return SQL(query: param, parameters: [value])
    }
    
    public func render(function: ForeignFunctionName, args: [ErasedRep], aliases: [String: String]) -> SQL {
        let args = args.map {($0, aliases)}.map(render(rep:aliases:)).joined(separator: ", ")
        return render(ff: function) + "(" + args + ")"
    }
    
    public func render(column: String, table: String, escape:Bool) -> SQL {
        let col = escape ? "`\(column)`" : column
        let sql = table == phony ? col : "\(table).\(col)"
        return SQL(query: sql, parameters: [])
    }
    
    public func render(table:Table, aliases: [String: String]) -> SQL {
        let alias = aliases[table.name]
        let text = alias.map { alias in
            "`\(table.name)` as \(alias)"
            } ?? "`\(table.name)`"
        
        return SQL(query: text, parameters: [])
    }
    
    public func render<J: JoinProtocol>(join:J, aliases: [String: String]) -> SQL {
        switch join.join {
        //CROSS
        case .cross(left: let left, right: let right):
            return left.render(renderer: self, aliases: aliases) + " CROSS JOIN " + right.render(renderer: self, aliases: aliases)
        //NATURAL
        case .inner(left: let left, right: let right, condition: .natural):
            return left.render(renderer: self, aliases: aliases) + " NATURAL JOIN " + right.render(renderer: self, aliases: aliases)
        //INNER with USING
        case .inner(left: let left, right: let right, condition: .using(let columns)):
            let using = columns.map {"`\($0)`"}.joined(separator: ", ")
            return left.render(renderer: self, aliases: aliases) + " INNER JOIN " + right.render(renderer: self, aliases: aliases) + " USING(\(using))"
        //INNER with ON
        case .inner(left: let left, right: let right, condition: .on(let predicate)):
            let on = predicate.render(renderer: self, aliases: aliases) ?? render(value: true)
            return left.render(renderer: self, aliases: aliases) + " INNER JOIN " + right.render(renderer: self, aliases: aliases) + " ON " + on
        //OUTER with USING
        case .outer(left: let left, right: let right, condition: .using(let columns), direction: let _direction):
            let direction = render(direction: _direction)
            let using = columns.map {"`\($0)`"}.joined(separator: ", ")
            return left.render(renderer: self, aliases: aliases) + " \(direction) OUTER JOIN " + right.render(renderer: self, aliases: aliases) + " USING(\(using))"
        //OUTER with ON
        case .outer(left: let left, right: let right, condition: .on(let predicate), direction: let _direction):
            let direction = render(direction: _direction)
            let on = predicate.render(renderer: self, aliases: aliases) ?? render(value: true)
            return left.render(renderer: self, aliases: aliases) + " \(direction) OUTER JOIN " + right.render(renderer: self, aliases: aliases) + " ON " + on
        default:
            fatalError("Not implemented")
        }
    }

    public func render(comparison op: BinaryOp, _ a:ErasedRep, _ b:ErasedRep, aliases: [String: String]) -> SQL {
        return render(op: op, render(rep: a, aliases: aliases), render(rep: b, aliases: aliases))
    }
    
    public func render(compound op: BinaryOp, _ a:Predicate, _ b:Predicate, aliases: [String: String]) -> SQL {
        let asql = a.render(renderer: self, aliases: aliases) ?? render(value: Optional<Any>.none)
        let bsql = b.render(renderer: self, aliases: aliases) ?? render(value: Optional<Any>.none)
        
        return render(op: op, asql, bsql)
    }
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

private extension SQLiteDialect {
    var param:String {
        return "?"
    }
    
    var phony:String {
        return "!"
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
        return rep.render(renderer: self, aliases: aliases)
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
        let tsql = table.render(renderer: self, aliases: [:])
        
        let phony = phonyAliases(dataset: table)
        
        let csql = render(columns: ret, aliases: phony)
        
        return "INSERT INTO " + tsql + " (" + csql + ") VALUES \n\t" + vsql
    }
    
    func render(ff: ForeignFunctionName) -> SQL {
        switch ff {
        case .lowercase:
            return "LOWER"
        case .uppercase:
            return "UPPER"
        case .custom(name: let name):
            return SQL(query: name, parameters: [])
        }
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
}
