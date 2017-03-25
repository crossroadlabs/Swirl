//===--- SQLite.swift ------------------------------------------------------===//
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

import Foundation
import Result
import Boilerplate
import RDBC

import CSQLite

private let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

public enum SQLiteError : Error {
    case unknown
    case code(Int32)
    case enhanced(code:Int32, message:String)
    case custom(message:String)
    case resource(name:String, message:String)
}

extension SQLiteError : RDBCError {
}

extension SQLiteError : ErrorWithCodeType {
    public typealias Code = Int32
    
    public init(code:Code) {
        self = .code(code)
    }
    
    public static func isError(code:Code) -> Bool {
        return code != SQLITE_OK
    }
}

internal extension SQLiteError {
    init(resource:OpaquePointer, name:String) {
        let message = String(validatingUTF8: sqlite3_errmsg(resource)) ?? "unknown error"
        self = .resource(name: name, message: message)
    }
}

extension SQLiteError : CustomStringConvertible {
    private var cdescription: UnsafePointer<Int8>? {
        switch self {
        case .unknown:
            return sqlite3_errmsg(nil)
        case .code(let code):
            return sqlite3_errstr(code)
        default:
            return nil
        }
    }
    
    public var description: String {
        switch self {
        case .custom(message: let message):
            return message
        case .enhanced(code: let code, message: let message):
            return "Code: \(code); \(message)"
        case .resource(name: let name, message: let message):
            return "\(name): \(message)"
        default:
            return cdescription.flatMap(String.init(validatingUTF8:)) ?? "unknown SQLiteError: error happened while trying to retreive the error message"
        }
    }
}

public protocol SQLiteObject : CObject, ErrorSource {
    associatedtype Object = OpaquePointer
    associatedtype Error = SQLiteError
}

public extension SQLiteObject where Object == OpaquePointer {
    public func error(code: Int32) -> SQLiteError {
        let message = String(validatingUTF8: with(sqlite3_errmsg))
        return message.map{.enhanced(code: code, message: $0)} ?? .code(code)
    }
}

public extension SQLiteObject {
    public static var ok:Int32 {
        return SQLITE_OK
    }
}

public class SQLiteDriver : SyncDriver {
    public let proto: String = "sqlite"
    
    public init() {
    }
    
    private func location(url _url:String) throws -> String {
        guard let url = URL(string: _url) else {
            throw SQLiteError.custom(message: "URL provided is an invalid SQLite URL: \(_url)")
        }
        
        let protoIsValid = url.scheme.map {proto == $0} ?? false
        
        if !protoIsValid {
            throw SQLiteError.custom(message: "URL provided doesn't use 'sqlite' protocol and thus can not be accepted: \(_url)")
        }
        
        if let host = url.host {
            switch host {
            case "temporary":
                return ""
            case "memory":
                return ":memory:"
            default:
                throw SQLiteError.custom(message: "URL provided is an invalid SQLite URL: \(_url). Only 'memory', 'temporary' or file path are allowed")
            }
        }
        
        return url.path
    }
    
    public func connect(url:String, params:Dictionary<String, String>) throws -> SyncConnection {
        let location = try self.location(url: url)
        
        let connection = try SQLiteConnection(location: location)
        try connection.set(timeout: 0.2)
        
        return connection
    }
}

private class StaticResultSet : SyncResultSet {
    private static let _initial: Int = -1
    
    private let _cols: [String]
    private let _rows: [[Any?]]
    private var _current: Int = StaticResultSet._initial
    
    init(cols: [String], rows: [[Any?]]) {
        _cols = cols
        _rows = rows
    }
    
    func columnCount() throws -> Int {
        return _cols.count
    }
    
    func columns() throws -> [String] {
        return _cols
    }
    
    func reset() throws {
        _current = StaticResultSet._initial
    }
    
    func next() throws -> [Any?]? {
        _current = _current.advanced(by: 1)
        return _current < _rows.count ? _rows[_current] : nil
    }
    
    static func count(_ count: Int) -> StaticResultSet {
        return StaticResultSet(cols: ["count"], rows: [[count]])
    }
}

public class SQLiteConnection : Resource<OpaquePointer>, SyncConnection, SQLiteObject {
    internal init(location:String) throws {
        var _connection:OpaquePointer? = nil
        try ccall(SQLiteError.self) { sqlite3_open(location, &_connection) }
        
        guard let connection = _connection else {
            throw SQLiteError.custom(message: "Connection didn't return... weird stuff...")
        }
        
        super.init(resource: connection, name: "SQLiteConnection") { (_connection:Resource<OpaquePointer>) in
            let connection = _connection as! SQLiteConnection
            try connection.call(sqlite3_close)
        }
    }
    
    internal func set(timeout: Timeout) throws {
        let millis = Int32(timeout.toMillis())
        try call {sqlite3_busy_timeout($0, millis)}
    }
    
    private var changes: Int {
        return Int(with(sqlite3_changes))
    }
    
    public func execute(query: String, parameters: [Any?], named: [String:Any?]) throws -> SyncResultSet? {
        let q = query.trimmingCharacters(in: CharacterSet.whitespacesAndNewlines)
        let statement = try SQLiteStatement(connection: self, query: q)
        
        try statement.bind(parameters: parameters)
        try statement.bind(named: named)
        
        let result:SyncResultSet? = try statement.step() ? SQLiteResultSet(statement: statement) : nil
        
        return result.or {
            if !(q.hasPrefix("INSERT") || q.hasPrefix("UPDATE") || q.hasPrefix("DELETE")) {
                return nil
            }
            
            let count = changes
            return StaticResultSet.count(count)
        }
    }
}

internal class SQLiteStatement : Resource<OpaquePointer>, SQLiteObject {
    init(connection:SQLiteConnection, query:String) throws {
        var connection:SQLiteConnection! = connection
        var _statement: OpaquePointer?
        //just leaving it here for later processing; doc here https://www.sqlite.org/c3ref/prepare.html
        var tail: UnsafePointer<Int8>? = nil
        
        // Prepare SQLite statement
        try connection.call { sqlite3_prepare_v2($0, query, -1, &_statement, &tail) }
        
        guard let statement = _statement else {
            throw SQLiteError.custom(message: "Statement didn't return... weird stuff...")
        }
        
        super.init(resource: statement, name: "SQLiteStatement") { (_statement:Resource<OpaquePointer>) in
            defer {
                connection = nil
            }
            let statement = _statement as! SQLiteStatement
            try statement.call(sqlite3_finalize)
        }
    }
    
    func bind(parameter: Any?, at index: Int32) throws {
        guard let parameter = parameter else {
            return try call {sqlite3_bind_null($0, index)}
        }
        
        switch parameter {
        case let value as String:
            return try call {sqlite3_bind_text($0, index, value, -1, SQLITE_TRANSIENT)}
        case let value as Float:
            return try call {sqlite3_bind_double($0, index, Double(value))}
        case let value as Double:
            return try call {sqlite3_bind_double($0, index, value)}
        case let value as Int:
            return try call {sqlite3_bind_int64($0, index, Int64(value))}
        case let value as Data:
            return try call {sqlite3_bind_blob($0, index, [UInt8](value), Int32(value.count), SQLITE_TRANSIENT)}
        default:
            let type = type(of: parameter)
            throw SQLiteError.custom(message: "Unsupported bound parameter type: \(type)")
        }
    }
    
    func bind(parameters: [Any?]) throws {
        for (i, parameter) in parameters.enumerated() {
            try bind(parameter: parameter, at: i+1)
        }
    }
    
    func bind(named parameters: [String:Any?]) throws {
        let indexed = try self.with { statement -> [(Int32, Any?)] in
            return try parameters.map { name, param in
                let index = sqlite3_bind_parameter_index(statement, name)
                
                guard index != 0 else {
                    throw SQLiteError.custom(message: "Named parameter \(name) is not found in query")
                }
                
                return (index, param)
            }
        }
        
        for (i, parameter) in indexed {
            try bind(parameter: parameter, at: i)
        }
    }
    
    //true indicates there is data to be processed
    func step() throws -> Bool {
        switch self.with(sqlite3_step) {
        case SQLITE_DONE:
            return false
        case SQLITE_ROW:
            return true
        case let code:
            throw error(code: code)
        }
    }
    
    func row(columns: Int) throws -> [Any?] {
        return try self.with { statement in
            try (0..<columns).map{Int32($0)}.map { column in
                //NOTE: do we need a better error checking here???
                switch sqlite3_column_type(statement, column) {
                case SQLITE_INTEGER:
                    return Int(sqlite3_column_int(statement, column))
                case SQLITE_FLOAT:
                    return sqlite3_column_double(statement, column)
                case SQLITE_TEXT:
                    let text = sqlite3_column_text(statement, column)
                    //any easier conversion possible???
                    return text.map(OpaquePointer.init).map(UnsafePointer<Int8>.init).flatMap(String.init(validatingUTF8:))
                case SQLITE_BLOB:
                    let count = sqlite3_column_bytes(statement, column)
                    guard count > 0 else {
                        return nil
                    }
                    
                    let bytes = sqlite3_column_blob(statement, column)
                    return bytes.map {Data(bytes: $0, count: Int(count))}
                case SQLITE_NULL:
                    return nil
                case let type:
                    throw SQLiteError.custom(message: "Column of type \(type) is currently unsupported")
                }
            }
        }
    }
    
    func columnCount() throws -> Int {
        return Int(with(sqlite3_column_count))
    }
    
    func columns(count:Int? = nil) throws -> [String] {
        let count = try count ?? columnCount()
        
        return try with { statement in
            try (0..<count).map{Int32($0)}.map { i in
                guard let name = String(validatingUTF8:sqlite3_column_name(statement, i)) else {
                    throw SQLiteError(resource: statement, name: "SQLiteStatement")
                }
                return name
            }
        }
    }
}

public class SQLiteResultSet : SyncResultSet {
    private let _statement:SQLiteStatement
    private lazy var _titles:Result<[String], AnyError> = { [unowned self] in
        return materialize(self.extractTitles)
    }()
    private lazy var _columnCount:Result<Int, AnyError> = { [unowned self] in
        return materialize(self._statement.columnCount)
    }()
    
    private var _first = true
    
    fileprivate init(statement: SQLiteStatement) {
        _statement = statement
    }
    
    private func extractTitles() throws -> [String] {
        let count = try columnCount()
        return try _statement.columns(count: count)
    }
    
    public func columnCount() throws -> Int {
        return try _columnCount.dematerializeAny()
    }
    
    public func columns() throws -> [String] {
        return try _titles.dematerializeAny()
    }
    
    public func reset() throws {
        try _statement.with { statement in
            try ccall(SQLiteError.self){sqlite3_reset(statement)}
        }
    }
    
    public func next() throws -> [Any?]? {
        if _first {
            _first = false
        } else if try !_statement.step() {
            return nil
        }
        
        return try _statement.row(columns: columnCount())
    }
}
