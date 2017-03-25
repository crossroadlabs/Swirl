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

public protocol Renderer {
    //back hooks for renderables
    func render<T>(value: T?) -> SQL
    func render(column: String, table: String, escape:Bool) -> SQL
    func render(table:Table, aliases: [String: String]) -> SQL
    func render<J: JoinProtocol>(join:J, aliases: [String: String]) -> SQL
    
    //predicates rendering
    func render(comparison op: BinaryOp, _ a:ErasedRep, _ b:ErasedRep, aliases: [String: String]) -> SQL
    func render(compound op: BinaryOp, _ a:Predicate, _ b:Predicate, aliases: [String: String]) -> SQL
}

public protocol Renderable {
    func render(renderer: Renderer, aliases: [String : String]) -> SQL
}

public extension ValueRep {
    public func render(renderer: Renderer, aliases: [String : String]) -> SQL {
        return renderer.render(value: value)
    }
}

public extension TupleRepProtocol {
    public func render(renderer: Renderer, aliases: [String : String]) -> SQL {
        fatalError("Can not render tuple rep")
    }
}

//kind of not exactly renderable in a standard way
public extension Predicate {
    public func render(renderer: Renderer, aliases:[String: String]) -> SQL? {
        switch self {
        case .null:
            return nil
        case .bool(let bool):
            return renderer.render(value: bool)
        case .comparison(op: let op, a: let a, b: let b):
            return renderer.render(comparison: op, a, b, aliases: aliases)
        case .compound(op: let op, let a, let b):
            return renderer.render(compound: op, a, b, aliases: aliases)
        }
    }
}

public extension Column {
    public func render(renderer: Renderer, aliases: [String : String]) -> SQL {
        let table = aliases[self.table.name] ?? self.table.name
        return renderer.render(column: name, table: table, escape: true)
    }
}

public extension Table {
    public func render(renderer: Renderer, aliases: [String: String]) -> SQL {
        return renderer.render(table: self, aliases: aliases)
    }
}

public extension JoinProtocol {
    public func render(renderer: Renderer, aliases:[String: String]) -> SQL {
        return renderer.render(join: self, aliases: aliases)
    }
}
