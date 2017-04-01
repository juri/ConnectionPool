import XCTest
import ConnectionPool

class ReserveOrWaitTests: XCTestCase {
    func test_reserve_wait_doesnt_block_with_unlimited() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: nil, maxIdleConnections: nil)
        XCTAssertNotNil(try pool.reserveOrWait(timeout: nil))
    }

    func test_reserve_wait_doesnt_block_when_under_limit() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)
        XCTAssertNotNil(try pool.reserveOrWait(timeout: nil))
    }

    func test_reserve_wait_blocks_free_unblocks() throws {
        let pool = DispatchPool<ClosureConnection>(connectionFactory: closureConnectionFactory, maxConnections: 1, maxIdleConnections: nil)

        let c1 = try pool.reserveOrWait(timeout: nil)!
        let expect = self.expectation(description: "Unblocked")
        var freeDone = false
        var secondReserveCalled = false
        var secondReserveReturned = false


        DispatchQueue.global().asyncAfter(deadline: .now() + 0.1) {
            DispatchQueue.main.asyncAfter(deadline: .now() + 0.1) {
                XCTAssertTrue(secondReserveCalled)
                XCTAssertFalse(secondReserveReturned)
                do {
                    try pool.free(c1)
                    freeDone = true
                } catch {
                    XCTFail("Got error freeing: \(error)")
                }
            }

            do {
                secondReserveCalled = true
                XCTAssertFalse(freeDone)
                let c2 = try pool.reserveOrWait(timeout: nil)
                XCTAssertTrue(freeDone)
                secondReserveReturned = true
                XCTAssertNotNil(c2)
                expect.fulfill()
            } catch {
                XCTFail("Got error reserving: \(error)")
            }
        }

        self.waitForExpectations(timeout: 0.3, handler: nil)
    }
}
