//===--- Swirl.swift ------------------------------------------------------===//
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

import Future

import RDBC

public enum SwirlError : Error {
    case noDialect
    case dialectDoesntMatchDriver
}

public class SwirlDriver {
    public let driver: Driver
    public let dialect: Dialect
    
    public init(driver: Driver, dialect: Dialect) throws {
        if driver.proto != dialect.proto {
            throw SwirlError.dialectDoesntMatchDriver
        }
        
        self.driver = driver
        self.dialect = dialect
    }
}

open class SyncSwirlDriver {
    public let driver: SyncDriver
    public let dialect: Dialect
    
    public init(driver: SyncDriver, dialect: Dialect) throws {
        if driver.proto != dialect.proto {
            throw SwirlError.dialectDoesntMatchDriver
        }
        
        self.driver = driver
        self.dialect = dialect
    }
}

public class SwirlManager {
    fileprivate let _rdbc:RDBC
    private var _dialects = [String:Dialect]()
    
    public init(rdbc:RDBC) {
        _rdbc = rdbc
    }
    
    public convenience init() {
        self.init(rdbc: RDBC())
    }
    
    public func register(driver: SyncSwirlDriver) {
        register(driver: driver.driver)
        register(dialect: driver.dialect)
    }
    
    public func register(driver: SwirlDriver) {
        register(driver: driver.driver)
        register(dialect: driver.dialect)
    }
    
    public func register(driver: Driver) {
        _rdbc.register(driver: driver)
    }
    
    public func register(driver: SyncDriver) {
        _rdbc.register(driver: driver)
    }
    
    public func register(dialect: Dialect) {
        _dialects[dialect.proto] = dialect
    }
    
    fileprivate func dialect(url:String, params: [String: String]) throws -> Dialect {
        let driver = try _rdbc.driver(url: url, params: params)
        guard let dialect = _dialects[driver.proto] else {
            throw SwirlError.noDialect
        }
        
        return dialect
    }
}

public typealias Renderlet = (Dialect) -> SQL

public class Swirl {
    private let _connection:Connection
    private let _dialect:Dialect
    private let _release:()->()
    
    init(connection: Connection, dialect: Dialect, release: @escaping ()->()) throws {
        _connection = connection
        _dialect = dialect
        _release = release
    }
    
    deinit {
        _release()
    }
    
    convenience init(connection: Connection, dialect: Dialect) throws {
        try self.init(connection: connection, dialect: dialect) {}
    }
    
    func execute(sql:SQL) -> Future<ResultSet?> {
        return _connection.execute(query: sql.query, parameters: sql.parameters, named: [:])
    }
    
    func render(renderlet: Renderlet) -> SQL {
        return renderlet(_dialect)
    }
    
    func execute(renderlet: Renderlet) -> Future<ResultSet?> {
        return execute(sql: render(renderlet: renderlet))
    }
    
    func execute<R>(_ f:(Dialect)->R) -> R {
        return f(_dialect)
    }
    
    var sequencial: Future<Swirl> {
        let pool = _connection as? ConnectionPool
        let connection = pool.map {$0.connection()} ?? Future(value: (_connection, {}))
        let dialect = _dialect
        return connection.map { (connection, release) in
            try Swirl(connection: connection, dialect: dialect, release: release)
        }
    }
}

public extension SwirlManager {
    public func swirl(url:String, params:[String: String] = [:]) throws -> Swirl {
        let dialect = try self.dialect(url: url, params: params)
        let pool = try _rdbc.pool(url: url, params: params)
        return try Swirl(connection: pool, dialect: dialect)
    }
}
