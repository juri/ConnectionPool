import Foundation
import Dispatch

public protocol Connection: class {
    func close() throws
}

public protocol Pool: class {
    associatedtype Conn

    func reserveOrWait(timeout: TimeInterval?) throws -> Conn?
    func reserveIfAvailable() throws -> Conn?
    func free(_ connection: Conn) throws
}

public enum PoolError: Error {
    case reserveFailed
}

public class DispatchPool<C: Connection>: Pool {
    public typealias Conn = C
    public typealias ConnectionFactory = (Void) throws -> C

    private let connectionFactory: ConnectionFactory
    private let maxConnections: Int?
    private let maxIdleConnections: Int?

    private let workQueue: DispatchQueue

    private var reservedConnectionCount: Int
    private var connections: ContiguousArray<Conn>
    private var returnPile: ContiguousArray<Conn>
    private var waiting: ContiguousArray<DispatchSemaphore>

    public init(connectionFactory: @escaping ConnectionFactory, maxConnections: Int?, maxIdleConnections: Int?) {
        self.connectionFactory = connectionFactory
        self.maxConnections = maxConnections
        self.maxIdleConnections = maxIdleConnections

        self.workQueue = DispatchQueue(
            label: "fi.juripakaste.ConnectionPool.DispatchPool.workQueue")
        self.connections = ContiguousArray()
        self.returnPile = ContiguousArray()
        self.waiting = ContiguousArray()
        self.reservedConnectionCount = 0
    }

    public func reserveOrWait(timeout: TimeInterval?) throws -> Conn? {
        let dispatchTimeout = timeout.map {
            DispatchTime.now() + DispatchTimeInterval.milliseconds(Int($0 * 1000))
        } ?? DispatchTime.distantFuture

        waitLoop: while true {
            let result: ReserveResult = try self.workQueue.sync {
                if let conn = try self.reserveConnection() {
                    return .connection(conn)
                }
                let semaphore = DispatchSemaphore(value: 0)
                self.waiting.append(semaphore)
                return .wait(semaphore)
            }
            switch result {
            case let .connection(conn):
                return conn
            case let .wait(sema):
                switch sema.wait(timeout: dispatchTimeout) {
                case .success: break
                case .timedOut: break waitLoop
                }
            }
        }
        return nil
    }

    public func reserveIfAvailable() throws -> Conn? {
        let conn = try self.workQueue.sync {
            return try self.reserveConnection()
        }
        return conn
    }

    public func free(_ connection: Conn) throws {
        try self.workQueue.sync {
            if let maxIdleConnections = self.maxIdleConnections, maxIdleConnections < self.returnPile.count {
                try connection.close()
                return
            }
            self.returnPile.append(connection)
            self.reservedConnectionCount -= 1
            self.signalWaiter()
        }
    }

    // Only call the private functions on workQueue

    private enum ReserveResult {
        case connection(C)
        case wait(DispatchSemaphore)
    }

    private func reserveConnection() throws -> C? {
        if let conn = self.reserveFreeConnection() {
            return conn
        }
        if self.returnPile.count > 0 {
            self.connections.append(contentsOf: self.returnPile.reversed())
            self.returnPile.removeAll()
            guard let conn = self.reserveFreeConnection() else {
                assertionFailure("Logic error: Didn't get a connection")
                return nil
            }
            return conn
        }
        if let conn = try self.maybeCreateConnection() {
            return conn
        }
        return nil
    }

    private func reserveFreeConnection() -> C? {
        guard let conn = self.connections.popLast() else {
            return nil
        }
        self.reservedConnectionCount += 1
        return conn
    }

    private func maybeCreateConnection() throws -> C? {
        if let maxConnections = self.maxConnections, self.totalConnections >= maxConnections {
            return nil
        }
        let conn = try self.connectionFactory()
        self.reservedConnectionCount += 1
        return conn
    }

    private func signalWaiter() {
        guard self.waiting.count > 0 else {
            return
        }
        let sema = self.waiting.removeFirst()
        sema.signal()
    }

    private var totalConnections: Int {
        return self.reservedConnectionCount +
            self.connections.count +
            self.returnPile.count
    }
}
