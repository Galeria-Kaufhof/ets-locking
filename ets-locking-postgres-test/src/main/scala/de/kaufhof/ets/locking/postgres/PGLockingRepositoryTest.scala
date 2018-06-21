package de.kaufhof.ets.locking.postgres

import java.time.{Clock, Instant}

import de.kaufhof.ets.locking.core.LockId
import doobie.scalatest.imports._
import org.scalatest.WordSpec

class PGLockingRepositoryTest extends WordSpec with DbTest with IOChecker {
  implicit val clk = Clock.systemDefaultZone()

  "PGLockingRepository" should {
    "create table" in {
      check(PGLockingRepository.createTableIfNotExistsSql(DbTest.tblName))
    }

    "find lock" in {
      check(PGLockingRepository.findLock(DbTest.tblName, LockId("test")))
    }

    "update lock" in {
      check(PGLockingRepository.upsertLock(DbTest.tblName, LockId("test"), LockInstanceId("testinstance"), Instant.now()))
    }

    "delete lock" in {
      check(PGLockingRepository.deleteLock(DbTest.tblName, LockId("test"), LockInstanceId("testinstance")))
    }
  }

}
