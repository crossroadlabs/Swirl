//===--- RDBC.swift ------------------------------------------------------===//
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
import Boilerplate
import ExecutionContext
import Future

public protocol ConnectionFactory {
    func connect(url:String, params:Dictionary<String, String>) -> Future<Connection>
}

public protocol PoolFactory {
    func pool(url:String, params:Dictionary<String, String>) -> ConnectionPool
}

public extension ConnectionFactory {
    func connect(url:String) -> Future<Connection> {
        return connect(url: url, params: [:])
    }
}

public extension PoolFactory {
    func pool(url:String) -> ConnectionPool {
        return pool(url: url, params: [:])
    }
}

public protocol Driver : ConnectionFactory {
    //can be mysql:// pgsql:// or whatever
    var proto:String {get}
}

public protocol Connection {
    func execute(query:String, parameters: [Any?], named: [String:Any?]) -> Future<ResultSet?>
}

public protocol ResultSet {
    typealias Row = [Any?]
    
    var columnCount:Future<Int> {get}
    var columns:Future<[String]> {get}
    
    func reset() -> Future<Void>
    func next() -> Future<Row?>
}

public extension ResultSet {
    private func accumulate(rows:[Row]) -> Future<[Row]> {
        return self.next().map { row in
            (rows, row.map {rows + [$0]})
        }.flatMap { (old, new) -> Future<[Row]> in
            if let new = new {
                return self.accumulate(rows: new)
            } else {
                return Future<[Row]>(value: old)
            }
        }
    }
    
    public func all() -> Future<[Row]> {
        return accumulate(rows: [Row]())
    }
}

public protocol SyncDriver {
    var proto:String {get}
    
    func connect(url:String, params:Dictionary<String, String>) throws -> SyncConnection
}

public protocol SyncConnection {
    @discardableResult
    func execute(query:String, parameters: [Any?], named: [String:Any?]) throws -> SyncResultSet?
}

public protocol SyncResultSet {
    func columnCount() throws -> Int
    func columns() throws -> [String]
    
    func reset() throws
    func next() throws -> [Any?]?
}

class AsyncDriver : Driver {
    private let _driver:SyncDriver
    private let _contextFactory:()->ExecutionContextProtocol
    
    init(driver:SyncDriver, contextFactory:@escaping ()->ExecutionContextProtocol) {
        _driver = driver
        _contextFactory = contextFactory
    }

    var proto:String {
        return _driver.proto
    }
    
    func connect(url:String, params:Dictionary<String, String>) -> Future<Connection> {
        let context = _contextFactory()
        let driver = _driver
        return future(context: context) {
            try driver.connect(url: url, params: params)
            //TODO: use zip instead of map{($0, context)}
        }.map{($0, context)}.map(AsyncConnection.init)
    }
}

class AsyncConnection : Connection, ExecutionContextTenantProtocol {
    private let _connection:SyncConnection
    
    let context: ExecutionContextProtocol
    
    init(connection:SyncConnection, context:ExecutionContextProtocol) {
        self.context = context
        self._connection = connection
    }
    
    func execute(query:String, parameters: [Any?], named: [String:Any?]) -> Future<ResultSet?> {
        let context = self.context
        let connection = _connection
        return future(context: context) {
            return try connection.execute(query: query, parameters: parameters, named: named)
        }.map { resultSet in
            resultSet.map{($0, context)}.map(AsyncResultSet.init)
        }
    }
}

class AsyncResultSet: ResultSet, ExecutionContextTenantProtocol {
    private let _resultSet:SyncResultSet
    
    let context: ExecutionContextProtocol
    
    init(resultSet:SyncResultSet, context:ExecutionContextProtocol) {
        self.context = context
        self._resultSet = resultSet
    }
    
    var columnCount:Future<Int> {
        let resultSet = _resultSet
        return future(context: context) { try resultSet.columnCount() }
    }
    
    var columns:Future<[String]> {
        let resultSet = _resultSet
        return future(context: context) { try resultSet.columns() }
    }
    
    func reset() -> Future<Void> {
        let resultSet = _resultSet
        return future(context: context) { try resultSet.reset() }
    }

    func next() -> Future<[Any?]?> {
        let resultSet = _resultSet
        return future(context: context) { try resultSet.next() }
    }
}

public enum RDBCError : Error {
    case invalid(url: String)
    case unknown(protocol: String)
    case noProtocol
}

public class ConnectionPool : Connection {
    private let _connectionFactory:()->Future<Connection>
    
    public init(connectionFactory:@escaping ()->Future<Connection>) {
        _connectionFactory = connectionFactory
    }
    
    public func execute(query: String, parameters: [Any?], named: [String : Any?]) -> Future<ResultSet?> {
        return _connectionFactory().flatMap { connection in
            connection.execute(query: query, parameters: parameters, named: named)
        }
    }
}

public class RDBC : ConnectionFactory, PoolFactory {
    private var _drivers = [String:Driver]()
    private let _contextFactory:()->ExecutionContextProtocol
    
    public init() {
        _contextFactory = {ExecutionContext(kind: .serial)}
    }
    
    public func register(driver: Driver) {
        _drivers[driver.proto] = driver
    }
    
    public func register(driver: SyncDriver) {
        let driver = AsyncDriver(driver: driver, contextFactory: _contextFactory)
        register(driver: driver)
    }
    
    public func pool(url:String, params:Dictionary<String, String>) -> ConnectionPool {
        return ConnectionPool {
            self.connect(url: url, params: params)
        }
    }
    
    public func connect(url _url: String, params: Dictionary<String, String>) -> Future<Connection> {
        guard let url = URL(string: _url) else {
            return Future(error: RDBCError.invalid(url: _url))
        }
        
        guard let proto = url.scheme else {
            return Future(error: RDBCError.noProtocol)
        }
        
        guard let driver = _drivers[proto] else {
            return Future(error: RDBCError.unknown(protocol: proto))
        }
        
        return driver.connect(url: _url, params: params)
    }
}
