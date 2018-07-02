package de.kaufhof.ets.locking.postgres

import java.sql.SQLException
import java.time.Clock
import java.util.concurrent.Executors

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future}
import scala.concurrent.stm._
import scala.util.{Failure, Success}
import akka.actor.{Cancellable, Scheduler}
import cats.effect.IO
import doobie._
import doobie.implicits._
import de.kaufhof.ets.locking.core._
import cats.implicits._
import doobie.postgres.sqlstate

import scala.math.Ordering.Implicits._

sealed trait ConnectionProvider
case class Datasource[A <: javax.sql.DataSource](datasource: A) extends ConnectionProvider
case class DriverManager(driver: String, url: String, user: String, password: String) extends ConnectionProvider

/**
  * Implementation for locking service using postgres
  * @param connectionProvider provies a connection for db access
  * @param tableName plain sql for table name (ATTENTION: vurnable to sql injections)!
  * @param blockingEC execution context for I/O via jdbc
  * @param ec default execution context
  * @param clock should be system time, used to calculate lock TTL
  */
class PGLockingService(connectionProvider: ConnectionProvider,
                       tableName: String = "ets-locking",
                       blockingEC: ExecutionContext = PGLockingService.defaultExecutionContext)
                      (implicit ec: ExecutionContext, scheduler: Scheduler, clock: Clock) extends LockingService {

  private val transactor: Transactor[IO] = connectionProvider match {
    case Datasource(d) => Transactor.fromDataSource.apply.apply(d)
    case DriverManager(driver, url, user, pw) => Transactor.fromDriverManager(driver, url, user, pw)
  }

  protected val lockingRepo = new PGLockingRepository(tableName)

  protected implicit class CioOps[T](cio: ConnectionIO[T]) {
    def execFuture: Future[T] = Future(cio.transact(transactor).unsafeRunSync())(blockingEC)
  }

  //if a lock will not be refreshed after this duration, another instance might acquire the lock
  protected val lockValidDuration: FiniteDuration = 60.seconds
  protected val lockRefreshInterval: FiniteDuration = lockValidDuration / 4
  protected val randomizeRefreshMax: FiniteDuration = lockRefreshInterval / 4

  //unique id referencing this instance
  protected val instanceId: LockInstanceId = LockInstanceId(java.util.UUID.randomUUID().toString)

  protected val locks = Ref(Set.empty[LockId])
  protected val locksWithPendingValidation = Ref(Set.empty[LockId])

  protected val refreshLockScheduled: Cancellable = scheduler.schedule(lockRefreshInterval, lockRefreshInterval)(refreshLocks())

  private def refreshLocks() : Unit = {
    //randomize to prevent multiple instances always try to refresh at the same time
    val delay = randomizeRefreshMax * (scala.math.random * 10.0).toLong / 10L
    val locksToRefresh = locks.single()

    //TODO use randomize-scheduler from TriggeringService
    scheduler.scheduleOnce(delay){
      val validUntil = clock.instant().plusMillis(lockValidDuration.toMillis)

      lockingRepo
        .refreshLocks(locksToRefresh, instanceId, validUntil)
        .execFuture
        .onComplete{
          case Success(locksRefreshed) =>
            val locksNotRefreshed = locksToRefresh -- locksRefreshed
            if (locksNotRefreshed.nonEmpty) {
              //log.error(epics.Core, s"Refresh failed for some locks: ${locksNotRefreshed.mkString(",")}")
              atomic {implicit txn =>
                locks() = locks() -- locksNotRefreshed
              }
            }
          case Failure(_) =>
            ()//log.error(epics.Core, "Refreshing locks failed", exc)
        }
    }

    ()
  }

  def createTableIfNotExists: Future[Unit] =
    lockingRepo.createTableIfNotExists.execFuture

  def withLock[T](lockId: LockId)(f: => Future[T]): Future[LockedExecution[T]] = {

    if (canAcquireLock(lockId)) {
      tryAcquireLock(lockId)
        .flatMap{ gotLock =>
          if (gotLock) {
            atomic {implicit txn =>
              locksWithPendingValidation() = locksWithPendingValidation() - lockId
              locks() = locks() + lockId
            }
            try {
              f.map(Executed.apply).andThen{case _ => releaseLock(lockId)}
            } catch {
              case exc: Throwable =>
                releaseLock(lockId)
                Future.failed(exc)
            }
          } else {
            atomic { implicit txn =>
              locksWithPendingValidation() = locksWithPendingValidation() - lockId
            }
            Future.successful(Locked)
          }
        }
    } else {
      Future.successful(Locked)
    }

  }

  private def releaseLock(lockId: LockId): Unit = {
    atomic { implicit txn =>
      locks() = locks() - lockId
    }
    lockingRepo
      .releaseLock(lockId, instanceId).execFuture
      .recoverWith {
        case exc: SQLException if exc.getSQLState == sqlstate.class40.SERIALIZATION_FAILURE.value =>
          //retry once when simultaneous access happened
          lockingRepo.releaseLock(lockId, instanceId).execFuture
      }
      .failed
      .foreach(_ => ()/*log.error(epics.Core, s"Releasing lock with id ${lockId.value} failed", exc)*/)
  }

  private def tryAcquireLock(lockId: LockId) = {
    lockAcquirable(lockId).flatMap{canAcquire =>
      if (canAcquire) {
        lockingRepo
          .acquireLock(lockId, instanceId, clock.instant().plusMillis(lockValidDuration.toMillis))
          .execFuture
          .recover {
            //if two instances try to acquire lock at the same time this error is raised by PG for one of them
            case exc: SQLException if exc.getSQLState == doobie.postgres.sqlstate.class40.SERIALIZATION_FAILURE.value =>
              false
          }
      } else {
        Future.successful(false)
      }
    }
  }

  private def lockAcquirable(lockId: LockId): Future[Boolean] = {
    //before trying to acquire a lock we check if that would be possible (with lower transaction isolation level) to reduce race conditions
    lockingRepo
      .getLockInfo(lockId)
      .map{
        case Some((_, validUntil)) =>
          validUntil < clock.instant()
        case None =>
          true
      }
      .execFuture
  }

  private def canAcquireLock(lockId: LockId) =
    atomic {implicit  txn =>
      if (locksWithPendingValidation().contains(lockId) || locks().contains(lockId)) {
        //lock exists
        false
      } else {
        //try to acquire a lock
        locksWithPendingValidation() = locksWithPendingValidation() + lockId
        true
      }
    }

}

object PGLockingService {
  def defaultExecutionContext: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))
}
