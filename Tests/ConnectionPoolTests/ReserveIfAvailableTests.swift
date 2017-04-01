import XCTest
import ConnectionPool

class ReserveIfAvailableTests: XCTestCase {
    func test_reserve_no_limits_succeeds() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: nil, maxIdleConnections: nil)
        XCTAssertNotNil(try pool.reserveIfAvailable())
    }

    func test_reserve_fails_with_zero_limit() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 0, maxIdleConnections: nil)
        XCTAssertNil(try pool.reserveIfAvailable())
    }

    func test_reserve_fails_when_over_limit() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)
        let c1 = try pool.reserveIfAvailable()
        XCTAssertNotNil(c1)
        let c2 = try pool.reserveIfAvailable()
        XCTAssertNil(c2)
    }

    func test_reserve_succeeds_when_freed_after_going_to_limit() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)
        let c1 = try pool.reserveIfAvailable()
        XCTAssertNotNil(c1)
        try pool.free(c1!)
        let c2 = try pool.reserveIfAvailable()
        XCTAssertNotNil(c2)
    }
}
