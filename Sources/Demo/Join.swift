//===--- Join.swift ------------------------------------------------------===//
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

public enum JoinDirection {
    case left
    case right
    case full
}

public enum JoinCondition {
    case on(Predicate)
    case using([String])
    case natural
}

extension JoinCondition : ExpressibleByArrayLiteral {
    public typealias Element = String
    
    /// Creates an instance initialized with the given elements.
    public init(arrayLiteral elements: String...) {
        self = .using(elements)
    }
}

public protocol JoinProtocol : Dataset {
    associatedtype Left : Dataset
    associatedtype Right : Table
    
    var join:Join<Left, Right> {get}
}

public extension JoinProtocol {
    public var tables:[Table] {
        let (left, right) = datasets
        return left.tables + right.tables
    }
}

public extension JoinProtocol {
    public func render(dialect:Dialect, aliases:[String: String]) -> SQL {
        return dialect.render(join: self, aliases: aliases)
    }
}

public extension JoinProtocol {
    public var datasets:(Left, Right) {
        switch join {
        case .cross(left: let left, right: let right):
            return (left, right)
        case .inner(left: let left, right: let right, condition: _):
            return (left, right)
        case .outer(left: let left, right: let right, condition: _, direction: _):
            return (left, right)
        }
    }
    
    func replace(left _left:Left? = nil, right _right:Right? = nil) -> Join<Left, Right> {
        switch join {
        case .cross(left: let left, right: let right):
            return .cross(left: _left ?? left, right: _right ?? right)
        case .inner(left: let left, right: let right, condition: let condition):
            return .inner(left: _left ?? left, right: _right ?? right, condition: condition)
        case .outer(left: let left, right: let right, condition: let condition, direction: let direction):
            return .outer(left: _left ?? left, right: _right ?? right, condition: condition, direction: direction)
        }
    }
}

public indirect enum Join<A : Dataset, B : Table> : JoinProtocol {
    public typealias Left = A
    public typealias Right = B
    
    case cross(left:Left, right:Right)
    case inner(left:Left, right:Right, condition:JoinCondition)
    case outer(left:Left, right:Right, condition:JoinCondition, direction:JoinDirection)
    
    public var join:Join {
        return self
    }
}
