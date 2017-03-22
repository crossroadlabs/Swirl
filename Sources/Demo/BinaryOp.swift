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
public func ==<T, RA : Rep>(a:RA, b:Null) -> Predicate where RA.Value == T {
    return .comparison(op: .eq, a: a, b: ValueRep(value: b))
}

public func ==<T, RB : Rep>(a:Null, b:RB) -> Predicate where RB.Value == T {
    return .comparison(op: .eq, a: ValueRep(value: b), b: b)
}

public func ==<RA : Rep, T : RDBCEquatable>(a:RA, b:T?) -> Predicate where RA.Value == T {
    return .comparison(op: .eq, a: a, b: ValueRep(value: b))
}

public func ==<T : RDBCEquatable>(a:ErasedColumn, b:T?) -> Predicate {
    return .comparison(op: .eq, a: a, b: ValueRep(value: b))
}

public func ==<T : RDBCEquatable, RA : Rep, RB : Rep>(a: RA, b: RB) -> Predicate where RA.Value == T, RB.Value == T {
    return .comparison(op: .eq, a: a, b: b)
}

public func ==(a:ErasedColumn, b: ErasedColumn) -> Predicate {
    return .comparison(op: .eq, a: a, b: b)
}

public func ==<T : RDBCEquatable, RB : Rep>(a:T?, b:RB) -> Predicate where RB.Value == T {
    return .comparison(op: .eq, a: ValueRep(value: a), b: b)
}

public func ==<T : RDBCEquatable>(a: T?, b: ErasedColumn) -> Predicate {
    return .comparison(op: .eq, a: ValueRep(value: a), b: b)
}

public func ==<T : Equatable>(a:T?, b:T?) -> Predicate {
    return .bool(a == b)
}

////////////////////////////////////////////////////////INEQUALITY////////////////////////////////////////////////////////
public func !=<T, RA : Rep>(a:RA, b:Null) -> Predicate where RA.Value == T {
    return .comparison(op: .neq, a: a, b: ValueRep(value: b))
}

public func !=<T, RB : Rep>(a:Null, b:RB) -> Predicate where RB.Value == T {
    return .comparison(op: .neq, a: ValueRep(value: b), b: b)
}

public func !=<RA : Rep, T : RDBCEquatable>(a:RA, b:T?) -> Predicate where RA.Value == T {
    return .comparison(op: .neq, a: a, b: ValueRep(value: b))
}

public func !=<T : RDBCEquatable>(a:ErasedColumn, b:T?) -> Predicate {
    return .comparison(op: .neq, a: a, b: ValueRep(value: b))
}

public func !=<T : RDBCEquatable, RA : Rep, RB : Rep>(a: RA, b: RB) -> Predicate where RA.Value == T, RB.Value == T {
    return .comparison(op: .neq, a: a, b: b)
}

public func !=(a:ErasedColumn, b: ErasedColumn) -> Predicate {
    return .comparison(op: .neq, a: a, b: b)
}

public func !=<T : RDBCEquatable, RB : Rep>(a:T?, b:RB) -> Predicate where RB.Value == T {
    return .comparison(op: .neq, a: ValueRep(value: a), b: b)
}

public func !=<T : RDBCEquatable>(a: T?, b: ErasedColumn) -> Predicate {
    return .comparison(op: .neq, a: ValueRep(value: a), b: b)
}

public func !=<T : Equatable>(a:T?, b:T?) -> Predicate {
    return .bool(a != b)
}

////////////////////////////////////////////////////////LIKE////////////////////////////////////////////////////////
public func ~=(a:ErasedColumn, b:ErasedColumn) -> Predicate {
    return .comparison(op: .like, a: a, b: b)
}

public func ~=<RA : Rep, RB : Rep>(a: RA, b: RB) -> Predicate where RA.Value == String, RB.Value == String {
    return .comparison(op: .like, a: a, b: b)
}

public func ~=(a: ErasedColumn, b: String) -> Predicate {
    return .comparison(op: .like, a: a, b: ValueRep(value: b))
}

public func ~=<RA : Rep>(a: RA, b: String) -> Predicate where RA.Value == String {
    return .comparison(op: .like, a: a, b: ValueRep(value: b))
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
public func ><T : RDBCComparable>(a:ErasedColumn, b: T) -> Predicate {
    return .comparison(op: .gt, a: a, b: ValueRep(value: b))
}

public func ><T : RDBCComparable, RA : Rep>(a: RA, b: T) -> Predicate where RA.Value == T {
    return .comparison(op: .gt, a: a, b: ValueRep(value: b))
}

public func >(a:ErasedColumn, b: ErasedColumn) -> Predicate {
    return .comparison(op: .gt, a: a, b: b)
}

public func ><T : RDBCComparable, RA : Rep, RB : Rep>(a: RA, b: RB) -> Predicate where RA.Value == T, RB.Value == T {
    return .comparison(op: .gt, a: a, b: b)
}

public func ><T : RDBCComparable>(a:T, b: ErasedColumn) -> Predicate {
    return .comparison(op: .gt, a: ValueRep(value: a), b: b)
}

public func ><T : RDBCComparable, RB : Rep>(a: T, b: RB) -> Predicate where RB.Value == T {
    return .comparison(op: .gt, a: ValueRep(value: a), b: b)
}

public func ><T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a > b)
}

////////////////////////////////////////////////////////LT////////////////////////////////////////////////////////

public func <<T : RDBCComparable>(a:ErasedColumn, b: T) -> Predicate {
    return .comparison(op: .lt, a: a, b: ValueRep(value: b))
}

public func <<T : RDBCComparable, RA : Rep>(a: RA, b: T) -> Predicate where RA.Value == T {
    return .comparison(op: .lt, a: a, b: ValueRep(value: b))
}

public func <(a:ErasedColumn, b: ErasedColumn) -> Predicate {
    return .comparison(op: .lt, a: a, b: b)
}

public func <<T : RDBCComparable, RA : Rep, RB : Rep>(a: RA, b: RB) -> Predicate where RA.Value == T, RB.Value == T {
    return .comparison(op: .lt, a: a, b: b)
}

public func <<T : RDBCComparable>(a:T, b: ErasedColumn) -> Predicate {
    return .comparison(op: .lt, a: ValueRep(value: a), b: b)
}

public func <<T : RDBCComparable, RB : Rep>(a: T, b: RB) -> Predicate where RB.Value == T {
    return .comparison(op: .lt, a: ValueRep(value: a), b: b)
}

public func <<T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a < b)
}

////////////////////////////////////////////////////////GTE////////////////////////////////////////////////////////
public func >=<T : RDBCComparable>(a:ErasedColumn, b: T) -> Predicate {
    return .comparison(op: .gte, a: a, b: ValueRep(value: b))
}

public func >=<T : RDBCComparable, RA : Rep>(a: RA, b: T) -> Predicate where RA.Value == T {
    return .comparison(op: .gte, a: a, b: ValueRep(value: b))
}

public func >=(a:ErasedColumn, b: ErasedColumn) -> Predicate {
    return .comparison(op: .gte, a: a, b: b)
}

public func >=<T : RDBCComparable, RA : Rep, RB : Rep>(a: RA, b: RB) -> Predicate where RA.Value == T, RB.Value == T {
    return .comparison(op: .gte, a: a, b: b)
}

public func >=<T : RDBCComparable>(a:T, b: ErasedColumn) -> Predicate {
    return .comparison(op: .gte, a: ValueRep(value: a), b: b)
}

public func >=<T : RDBCComparable, RB : Rep>(a: T, b: RB) -> Predicate where RB.Value == T {
    return .comparison(op: .gte, a: ValueRep(value: a), b: b)
}

public func >=<T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a >= b)
}

////////////////////////////////////////////////////////LTE////////////////////////////////////////////////////////

public func <=<T : RDBCComparable>(a:ErasedColumn, b: T) -> Predicate {
    return .comparison(op: .lte, a: a, b: ValueRep(value: b))
}

public func <=<T : RDBCComparable, RA : Rep>(a: RA, b: T) -> Predicate where RA.Value == T {
    return .comparison(op: .lte, a: a, b: ValueRep(value: b))
}

public func <=(a:ErasedColumn, b: ErasedColumn) -> Predicate {
    return .comparison(op: .lte, a: a, b: b)
}

public func <=<T : RDBCComparable, RA : Rep, RB : Rep>(a: RA, b: RB) -> Predicate where RA.Value == T, RB.Value == T {
    return .comparison(op: .lte, a: a, b: b)
}

public func <=<T : RDBCComparable>(a:T, b: ErasedColumn) -> Predicate {
    return .comparison(op: .lte, a: ValueRep(value: a), b: b)
}

public func <=<T : RDBCComparable, RB : Rep>(a: T, b: RB) -> Predicate where RB.Value == T {
    return .comparison(op: .lte, a: ValueRep(value: a), b: b)
}

public func <=<T : Comparable>(a:T, b:T) -> Predicate {
    return .bool(a <= b)
}
