//===--- BinaryOp.swift ------------------------------------------------------===//
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

public enum BinaryOp {
    case and
    case or
    case xor
    
    case eq
    case neq
    case like
    
    case gt
    case lt
    case gte
    case lte
}

public protocol RDBCEquatable : Equatable {
}

public protocol RDBCComparable : RDBCEquatable, Comparable {
}

extension Bool : RDBCEquatable {
}

extension String : RDBCEquatable {
}

extension Int : RDBCComparable {
}

extension Double : RDBCComparable {
}

extension Float : RDBCComparable {
}

////////////////////////////////////////////////////////EQUALITY////////////////////////////////////////////////////////
public func ==(column:Column, value:Null) -> Predicate {
    return .comparison(op: .eq, a: MetaValue<Any>.column(column), b: value.meta)
}

public func ==(value:Null, column:Column) -> Predicate {
    return .comparison(op: .eq, a: value.meta, b: MetaValue<Any>.column(column))
}

public func ==<T : RDBCEquatable>(column:Column, value:T?) -> Predicate {
    return .comparison(op: .eq, a: MetaValue<Any>.column(column), b: value.map {MetaValue.static($0)})
}

public func ==(column1:Column, column2:Column) -> Predicate {
    return .comparison(op: .eq, a: MetaValue<Any>.column(column1), b: MetaValue<Any>.column(column2))
}

public func ==<T : RDBCEquatable>(value:T?, column:Column) -> Predicate {
    return .comparison(op: .eq, a: value.map {MetaValue.static($0)}, b: MetaValue<Any>.column(column))
}

public func ==<T : Equatable>(a:T?, b:T?) -> Predicate {
    return .bool(a == b)
}

////////////////////////////////////////////////////////INEQUALITY////////////////////////////////////////////////////////
public func !=(column:Column, value:Null) -> Predicate {
    return .comparison(op: .neq, a: MetaValue<Any>.column(column), b: value.meta)
}

public func !=(value:Null, column:Column) -> Predicate {
    return .comparison(op: .neq, a: value.meta, b: MetaValue<Any>.column(column))
}

public func !=<T : RDBCEquatable>(column:Column, value:T?) -> Predicate {
    return .comparison(op: .neq, a: MetaValue<Any>.column(column), b: value.map {MetaValue.static($0)})
}

public func !=(column1:Column, column2:Column) -> Predicate {
    return .comparison(op: .neq, a: MetaValue<Any>.column(column1), b: MetaValue<Any>.column(column2))
}

public func !=<T : RDBCEquatable>(value:T?, column:Column) -> Predicate {
    return .comparison(op: .neq, a: value.map {MetaValue.static($0)}, b: MetaValue<Any>.column(column))
}

public func !=<T : Equatable>(a:T?, b:T?) -> Predicate {
    return .bool(a != b)
}

////////////////////////////////////////////////////////LIKE////////////////////////////////////////////////////////
public func ~=(a:Column, b:Column) -> Predicate {
    return .comparison(op: .like, a: MetaValue<Any>.column(a), b: MetaValue<Any>.column(b))
}

public func ~=(column:Column, value:String) -> Predicate {
    return .comparison(op: .like, a: MetaValue<Any>.column(column), b: MetaValue.static(value))
}

////////////////////////////////////////////////////////AND////////////////////////////////////////////////////////
public func &&(p1:Predicate, p2:Predicate) -> Predicate {
    switch (p1, p2) {
    case (.null, .null):
        return .null
    case (.null, let p2):
        return p2
    case (let p1, .null):
        return p1
    case (.bool(let p1), .bool(let p2)):
        return p1 && p2
    case (let p1, .bool(let p2)):
        return p1 && p2
    case (.bool(let p1), let p2):
        return p1 && p2
    default:
        return .compound(op: .and, p1, p2)
    }
}

public func &&(p1:Predicate, p2:Bool) -> Predicate {
    return p2 ? p1 : .bool(false)
}

public func &&(p1:Bool, p2:Predicate) -> Predicate {
    return p1 ? p2 : .bool(false)
}

public func &&(p1:Bool, p2:Bool) -> Predicate {
    return .bool(p1 && p2)
}

////////////////////////////////////////////////////////OR////////////////////////////////////////////////////////
public func ||(p1:Predicate, p2:Predicate) -> Predicate {
    switch (p1, p2) {
    case (.null, .null):
        return .null
    case (.null, let p2):
        return p2
    case (let p1, .null):
        return p1
    case (.bool(let p1), .bool(let p2)):
        return p1 || p2
    case (let p1, .bool(let p2)):
        return p1 || p2
    case (.bool(let p1), let p2):
        return p1 || p2
    default:
        return .compound(op: .or, p1, p2)
    }
}

public func ||(p1:Predicate, p2:Bool) -> Predicate {
    return p1
}

public func ||(p1:Bool, p2:Predicate) -> Predicate {
    return p2
}

public func ||(p1:Bool, p2:Bool) -> Predicate {
    return .bool(p1 || p2)
}

////////////////////////////////////////////////////////XOR////////////////////////////////////////////////////////
public func !=(p1:Predicate, p2:Predicate) -> Predicate {
    switch (p1, p2) {
    case (.bool(let p1), .bool(let p2)):
        return .bool(p1 != p2)
    case (let p1, .bool(let p2)):
        return p1 != p2
    case (.bool(let p1), let p2):
        return p1 != p2
    default:
        return .compound(op: .xor, p1, p2)
    }
}

public func !=(p1:Bool, p2:Predicate) -> Predicate {
    return .compound(op: .xor, .bool(p1), p2)
}

public func !=(p1:Predicate, p2:Bool) -> Predicate {
    return .compound(op: .xor, p1, .bool(p2))
}

public func !=(p1:Bool, p2:Bool) -> Predicate {
    return .bool(p1 != p2)
}

////////////////////////////////////////////////////////GT////////////////////////////////////////////////////////
public func ><T : RDBCComparable>(column:Column, value:T) -> Predicate {
    return .comparison(op: .gt, a: MetaValue<Any>.column(column), b: MetaValue.static(value))
}

public func >(column1:Column, column2:Column) -> Predicate {
    return .comparison(op: .gt, a: MetaValue<Any>.column(column1), b: MetaValue<Any>.column(column2))
}

public func ><T : RDBCComparable>(value:T, column:Column) -> Predicate {
    return .comparison(op: .gt, a: MetaValue.static(value), b: MetaValue<Any>.column(column))
}

public func ><T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a > b)
}

////////////////////////////////////////////////////////LT////////////////////////////////////////////////////////
public func <<T : RDBCComparable>(column:Column, value:T) -> Predicate {
    return .comparison(op: .lt, a: MetaValue<Any>.column(column), b: MetaValue.static(value))
}

public func <(column1:Column, column2:Column) -> Predicate {
    return .comparison(op: .lt, a: MetaValue<Any>.column(column1), b: MetaValue<Any>.column(column2))
}

public func <<T : RDBCComparable>(value:T, column:Column) -> Predicate {
    return .comparison(op: .lt, a: MetaValue.static(value), b: MetaValue<Any>.column(column))
}

public func <<T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a < b)
}

////////////////////////////////////////////////////////GTE////////////////////////////////////////////////////////
public func >=<T : RDBCComparable>(column:Column, value:T) -> Predicate {
    return .comparison(op: .gte, a: MetaValue<Any>.column(column), b: MetaValue.static(value))
}

public func >=(column1:Column, column2:Column) -> Predicate {
    return .comparison(op: .gte, a: MetaValue<Any>.column(column1), b: MetaValue<Any>.column(column2))
}

public func >=<T : RDBCComparable>(value:T, column:Column) -> Predicate {
    return .comparison(op: .gte, a: MetaValue.static(value), b: MetaValue<Any>.column(column))
}

public func >=<T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a >= b)
}

////////////////////////////////////////////////////////LTE////////////////////////////////////////////////////////
public func <=<T : RDBCComparable>(column:Column, value:T) -> Predicate {
    return .comparison(op: .lte, a: MetaValue<Any>.column(column), b: MetaValue.static(value))
}

public func <=(column1:Column, column2:Column) -> Predicate {
    return .comparison(op: .lte, a: MetaValue<Any>.column(column1), b: MetaValue<Any>.column(column2))
}

public func <=<T : RDBCComparable>(value:T, column:Column) -> Predicate {
    return .comparison(op: .lte, a: MetaValue.static(value), b: MetaValue<Any>.column(column))
}

public func <=<T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a <= b)
}
