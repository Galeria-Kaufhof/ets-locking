package de.kaufhof.ets.locking.postgres

import java.time.{Clock, Instant, ZoneId}

import akka.actor.{ActorSystem, Scheduler}
import de.kaufhof.ets.locking.core.{Executed, LockId, Locked}
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration._
import scala.concurrent.stm.{Ref, atomic}
import scala.concurrent.{Await, Future}
import scala.util.Try
import org.scalatest.concurrent.Eventually
import scala.language.reflectiveCalls
import scala.concurrent.ExecutionContext.Implicits.global
import doobie.implicits._
import cats.implicits._

class PGLockingServiceTest extends WordSpec with Matchers with Eventually with DbTest {
  implicit val as = ActorSystem("PGLockingServiceTest")
  implicit val scheduler = as.scheduler

  def await[T](f: => Future[T]): T = Await.result(f, 5.seconds)
  def awaitTry[T](f: => Future[T]): Try[T] = Try(await(f))

  val testtime = Instant.now
  val testLockId = LockId("testlock")
  val fastLockRefreshInterval = 1.seconds

  def fixture = new {
    implicit val mutableClock = new MutableClock
    mutableClock.setTime(testtime)

    val testee = new PGLockingServiceWithInsights(connectionProvider)
    lazy val secondTesteeInstance = new PGLockingServiceWithInsights(connectionProvider)
    lazy val fastRefreshingTestee = new PGLockingServiceWithInsights(connectionProvider, fastLockRefreshInterval, 0.seconds)
    val lockingRepo = new PGLockingRepository(DbTest.tblName)

    def shutdown() = {
      testee.shutdown()
      secondTesteeInstance.shutdown()
      fastRefreshingTestee.shutdown()
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()
    as.terminate()
    ()
  }


  "PGLockingService" should {

    "execute code when no lock exists and release lock after that"  in {
      val f = fixture
      var lockCodeWasExecuted = false

      await(f.testee.withLock(testLockId){Future{lockCodeWasExecuted = true}})

      lockCodeWasExecuted shouldEqual true
      f.testee.getLocks shouldEqual Set.empty[LockId]

      eventually(timeout(1.seconds), interval(100.milliseconds)) {
        f.lockingRepo.getLockInfo(testLockId).execSync shouldEqual None
      }

      f.shutdown()
    }

    "execute code for one lock once for same instance" in {
      val f = fixture
      var lockCodeWasExecuted1 = false
      var lockCodeWasExecuted2 = false

      val res1 = f.testee.withLock(testLockId){Future{Thread.sleep(200); lockCodeWasExecuted1 = true}}
      val res2 = f.testee.withLock(testLockId){Future{Thread.sleep(200); lockCodeWasExecuted2 = true}}

      await(res1) shouldEqual Executed(())
      await(res2) shouldEqual Locked

      lockCodeWasExecuted1 shouldEqual true
      lockCodeWasExecuted2 shouldEqual false

      f.shutdown()
    }

    "not execute if locked and lock did not time out" in {
      val f = fixture
      f.lockingRepo.acquireLock(testLockId, LockInstanceId("blockTheLock"), f.mutableClock.instant()).execSync

      var lockCodeWasExecuted = false

      await(f.testee.withLock(testLockId){Future{lockCodeWasExecuted = true}})

      lockCodeWasExecuted shouldEqual false

      f.shutdown()
    }

    "execute if locked but lock did time out" in {
      val f = fixture
      f.lockingRepo.acquireLock(testLockId, LockInstanceId("blockTheLock"), f.mutableClock.instant()).execSync shouldEqual true

      var lockCodeWasExecuted = false

      f.mutableClock.addTime(1.seconds)

      await(f.testee.withLock(testLockId){Future{lockCodeWasExecuted = true}}) shouldEqual Executed(())

      lockCodeWasExecuted shouldEqual true

      f.shutdown()
    }

    "release lock if user function fails" in {
      val f = fixture

      awaitTry(f.testee.withLock(testLockId){Future.failed(new Exception("Test"))})

      eventually(timeout(1.seconds), interval(100.milliseconds)) {
        f.lockingRepo.getLockInfo(testLockId).execSync shouldEqual None
      }

      f.shutdown()
    }

    "release lock if user function fails synchronous" in {
      val f = fixture

      awaitTry(f.testee.withLock(testLockId){throw new Exception("Test")})

      eventually(timeout(1.seconds), interval(100.milliseconds)) {
        f.lockingRepo.getLockInfo(testLockId).execSync shouldEqual None
      }

      f.shutdown()
    }

    "refresh locks when function runs longer than log expiration" in {
      val f = fixture
      f.mutableClock.setUseSystemTime(true)

      f.fastRefreshingTestee.withLock(testLockId){Future{Thread.sleep(3000)}}

      var lockInfos = List.empty[(LockInstanceId, Instant)]

      eventually(timeout(5.seconds), interval(300.milliseconds)) {
        lockInfos = lockInfos ++ f.lockingRepo.getLockInfo(testLockId).execSync.toList

        val validUntils = lockInfos.map(_._2).distinct

        validUntils.size should be >= 2
      }

      f.shutdown()
    }

    "only try to acquire lock if it is not held or invalid" in {
      val f = fixture
      f.testee.lockingRepoWithInsights.acquireLock(testLockId, LockInstanceId("blockTheLock"), f.mutableClock.instant()).execSync shouldEqual true

      f.testee.lockingRepoWithInsights.triedToAcquireLock = false

      var lockCodeWasExecuted = false

      awaitTry(f.testee.withLock(testLockId){Future{lockCodeWasExecuted = true}})

      lockCodeWasExecuted shouldEqual false
      f.testee.lockingRepoWithInsights.triedToAcquireLock shouldEqual false

      f.shutdown()
    }

  }

}

class PGLockingRepositoryWithInsights(tableName: String)(implicit clock: Clock) extends PGLockingRepository(tableName)(clock) {
  var triedToAcquireLock = false
  override def acquireLock(lockId: LockId, instanceId: LockInstanceId, validUntil: Instant): doobie.ConnectionIO[Boolean] = {
    triedToAcquireLock = true
    super.acquireLock(lockId, instanceId, validUntil)
  }
}


class PGLockingServiceWithInsights(connectionProvider: ConnectionProvider,
                                   override protected val lockRefreshInterval: FiniteDuration = 15.seconds,
                                   override protected val randomizeRefreshMax: FiniteDuration = 5.seconds)
                                  (implicit scheduler: Scheduler, clock: Clock)
  extends PGLockingService(connectionProvider, DbTest.tblName) {

  lazy val lockingRepoWithInsights = new PGLockingRepositoryWithInsights(DbTest.tblName)

  override val lockingRepo: PGLockingRepository = lockingRepoWithInsights

  def getLocks = locks.single()

  def shutdown(): Unit = {
    refreshLockScheduled.cancel()
     Await.result(locks.single().map(lockingRepo.releaseLock(_, instanceId)).toList.sequence.execFuture, 10.seconds)
    ()
  }

}

class MutableClock extends Clock {

  protected val theTime  = Ref(Instant.now())

  protected val useSystemTime = Ref(false)

  def setUseSystemTime(useSysTime: Boolean): Unit =
    useSystemTime.single() = useSysTime

  override def withZone(zone: ZoneId): Clock = this

  override def getZone: ZoneId = Clock.systemDefaultZone().getZone

  override def instant(): Instant =
    if (useSystemTime.single()) {
      Instant.now()
    } else {
      theTime.single()
    }

  def setTime(newTime: Instant): Unit = theTime.single() = newTime

  def addTime(d: FiniteDuration): Unit = atomic {implicit txn =>
    theTime() = theTime().plusMillis(d.toMillis)
  }

}
