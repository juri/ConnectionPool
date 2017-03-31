import XCTest
import ConnectionPool

class ReserveOrWaitTimeoutTests: XCTestCase {
    func test_wait_timeout() throws {
        let factory = ClosureConnectionFactory(connectionCloser: {})
        let pool = DispatchPool(connectionFactory: factory, maxConnections: 1, maxIdleConnections: nil)
        let expectation = self.expectation(description: "Unblocked")
        DispatchQueue.global().async {
            do {
                let c1 = try pool.reserveOrWait(timeout: nil)
                XCTAssertNotNil(c1)
                let d1 = Date()
                let c2 = try pool.reserveOrWait(timeout: 1)
                XCTAssertNil(c2)
                let d2 = Date()
                let duration = d2.timeIntervalSince(d1)
                XCTAssertGreaterThan(duration, 1.0)
                XCTAssertLessThan(duration, 1.1)
                expectation.fulfill()
            } catch {
                XCTFail("Unexpected error \(error)")
            }
        }

        self.waitForExpectations(timeout: 1.3, handler: nil)
    }
}
