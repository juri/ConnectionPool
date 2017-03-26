import Foundation
import Dispatch

public protocol Connection: class {
    func close() throws
}

public protocol ConnectionFactory {
    func connection() throws -> Connection
}

public protocol Pool: class {
    func reserveOrWait(timeout: TimeInterval?) throws -> Connection?
    func reserveIfAvailable() throws -> Connection?
    func free(_ connection: Connection) throws
}

public enum PoolError: Error {
    case reserveFailed
}

public class DispatchPool: Pool {
    private let connectionFactory: ConnectionFactory
    private let maxConnections: Int?
    private let maxIdleConnections: Int?

    private let workQueue: DispatchQueue

    private var reservedConnectionCount: Int
    private var connections: ContiguousArray<Connection>
    private var returnPile: ContiguousArray<Connection>
    private var waiting: ContiguousArray<DispatchSemaphore>

    init(connectionFactory: ConnectionFactory, maxConnections: Int?, maxIdleConnections: Int?) {
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

    public func reserveOrWait(timeout: TimeInterval?) throws -> Connection? {
        enum ReserveResult {
            case connection(Connection)
            case wait(DispatchSemaphore)
        }
        let start = Date()
        var notOverTimeout: Bool {
            guard let t = timeout else { return true }
            return Date().timeIntervalSince(start) < t
        }

        while notOverTimeout {
            let result: ReserveResult = self.workQueue.sync {
                if let conn = self.connections.popLast() {
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
                sema.wait()
            }
        }
        return nil
    }

    public func reserveIfAvailable() throws -> Connection? {
        let conn = self.workQueue.sync {
            return self.connections.popLast()
        }
        return conn
    }

    public func free(_ connection: Connection) throws {
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

    private func reserveConnection() throws -> Connection? {
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

    private func reserveFreeConnection() -> Connection? {
        guard let conn = self.connections.popLast() else {
            return nil
        }
        self.reservedConnectionCount += 1
        return conn
    }

    private func maybeCreateConnection() throws -> Connection? {
        if let maxConnections = self.maxConnections, self.totalConnections < maxConnections {
            return nil
        }
        let conn = try self.connectionFactory.connection()
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
