//===--- Dialect.swift ------------------------------------------------------===//
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

public protocol Dialect {
    var proto: String {get}
    
    //affected rows count key
    var affected: String {get}
    
    func render<DS: Dataset, Ret : Rep>(select ret: Ret, from dataset:DS, filter:Predicate, limit:Limit?) -> SQL
    
    //inserts
    func render<DS: Table, Ret: Rep>(insert row: [ErasedRep], into table:DS, ret: Ret) -> SQL
    func render<DS: Table, Ret: Rep>(insert rows: [[ErasedRep]], into table:DS, ret: Ret) -> SQL
    
    //update
    func render<DS: Table, Ret: Rep>(update values: [ErasedRep], into table:DS, ret: Ret, matching: Predicate) -> SQL
}
