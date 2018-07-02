package de.kaufhof.ets.locking.local

import de.kaufhof.ets.locking.core.{Executed, LockId, Locked}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Try}

class LocalLockingServiceTest extends WordSpec with Matchers {
  import scala.concurrent.ExecutionContext.Implicits.global

  "LocalLockingService" should {

    "execute code when no lock exists" in new Fixture {

      var executed = false

      await(localLockingService.withLock(lockId)(Future{executed = true})) shouldEqual Executed(())

      executed shouldEqual true
    }

    "not execute code when lock exists" in new Fixture {
      var executed = false

      localLockingService.withLock(lockId)(futureToWait)

      await(localLockingService.withLock(lockId)(Future{executed = true})) shouldEqual Locked

      promiseToWait.trySuccess(())

      executed shouldEqual false
    }

    "release lock on sync execption" in new Fixture {
      var executed = false

      awaitCatch(localLockingService.withLock(lockId)(throw TestException)) shouldEqual Failure(TestException)

      await(localLockingService.withLock(lockId)(Future{executed = true})) shouldEqual Executed(())

      executed shouldEqual true
    }

    "release lock on async exception" in new Fixture {
      var executed = false

      awaitCatch(localLockingService.withLock(lockId)(Future.failed(TestException))) shouldEqual Failure(TestException)

      await(localLockingService.withLock(lockId)(Future{executed = true})) shouldEqual Executed(())

      executed shouldEqual true
    }

  }

  trait Fixture {
    val lockId = LockId("test")
    val localLockingService = new LocalLockingService()

    val promiseToWait = Promise[Unit]
    val futureToWait = promiseToWait.future

    def await[T](f: Future[T], timeout: FiniteDuration = 5.seconds): T = Await.result(f, timeout)
    def awaitCatch[T](f: Future[T], timeout: FiniteDuration = 5.seconds): Try[T] = Try(Await.result(f, timeout))
  }

  case object TestException extends Exception("")

}
