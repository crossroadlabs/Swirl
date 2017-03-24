//===--- SyncAdapter.swift ------------------------------------------------------===//
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
import Future

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
