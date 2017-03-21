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

@_exported import enum Boilerplate.Null
import Boilerplate
import Future

import RDBC

public enum SwirlError : Error {
    case noDialect
    case dialectDoesntMatchDriver
}

public struct SwirlDriver {
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

public class SwirlManager {
    fileprivate let _rdbc:RDBC
    private var _dialects = [String:Dialect]()
    
    public init(rdbc:RDBC) {
        _rdbc = rdbc
    }
    
    public convenience init() {
        self.init(rdbc: RDBC())
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
    
    fileprivate func driver(url:String, params: [String: String]) throws -> SwirlDriver {
        let driver = try _rdbc.driver(url: url, params: params)
        guard let dialect = _dialects[driver.proto] else {
            throw SwirlError.noDialect
        }
        
        return try SwirlDriver(driver: driver, dialect: dialect)
    }
}

public protocol Dialect {
    var proto:String {get}
}

public class Swirl {
    private let _pool:ConnectionPool
    private let _dialect:Dialect
    
    init(pool:ConnectionPool, dialect:Dialect) throws {
        _pool = pool
        _dialect = dialect
    }
    
    func execute(sql:SQL) -> Future<ResultSet?> {
        return _pool.execute(query: sql.query, parameters: sql.parameters, named: [:])
    }
    
    func execute<Q : Query>(query:Q) -> Future<ResultSet?> {
        return self.execute(sql: query.render(dialect: _dialect))
    }
}

public extension SwirlManager {
    public func swirl(url:String, params:[String: String] = [:]) throws -> Swirl {
        let dialect = try driver(url: url, params: params).dialect
        let pool = _rdbc.pool(url: url, params: params)
        return try Swirl(pool: pool, dialect: dialect)
    }
}