package de.kaufhof.ets.locking.local

import de.kaufhof.ets.locking.core._

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.stm._
import scala.util.Try

class LocalLockingService(ec: ExecutionContext = ExecutionContext.global) extends LockingService {

  implicit val implEc: ExecutionContext = ec
  
  val locks = Ref(Set.empty[LockId])

  override def withLock[T](lockId: LockId)(f: => Future[T]): Future[LockedExecution[T]] = {

    val iGotTheLock = atomic {implicit txn =>
      if (locks().contains(lockId)) {
        false
      } else {
        locks() = locks() + lockId
        true
      }
    }

    if (iGotTheLock) {
      Try(f).recover{ case err => Future.failed(err)}
        .get
        .map(Executed.apply)
        .andThen{
          case _ => atomic {implicit txn =>
            locks() = locks() - lockId
          }
        }
    } else {
      Future.successful(Locked)
    }

  }

}
