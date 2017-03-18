//===--- PG.swift ------------------------------------------------------===//
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

import CLibpq

public enum PostgresError : Error {
    case unknown(message:String)
    case error(message:String)
}

public class PostgresDriver : SyncDriver {
    public let proto: String = "pgsql"

    public func connect(url:String, params:Dictionary<String, String>) throws -> SyncConnection {
        let conninfo = ""//actually convert url+params to conninfo
        
        guard let connection = PQconnectdb(conninfo) else {
            throw PostgresError.unknown(message: "PQconnectdb returned null")
        }
        
        if let error = String(validatingUTF8: PQerrorMessage(connection)), !error.isEmpty {
            throw PostgresError.error(message: error)
        }
        
        return PostgresConnection(connection: connection)
    }
}

public class PostgresConnection : SyncConnection {
    private let _connection:OpaquePointer
    
    public init(connection:OpaquePointer) {
        _connection = connection
    }
    
    public func execute(query: String, parameters: [Any?], named: [String:Any?]) throws -> SyncResultSet? {
        PQsendQuery(_connection, query)
        PQsetSingleRowMode(_connection)
        return nil
    }
}
