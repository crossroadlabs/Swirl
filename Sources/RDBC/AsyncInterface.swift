//===--- AsyncInterface.swift ------------------------------------------------------===//
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
