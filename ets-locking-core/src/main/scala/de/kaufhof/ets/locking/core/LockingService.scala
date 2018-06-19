package de.kaufhof.ets.locking.core

import scala.concurrent.Future

case class LockId(value: String)

sealed trait LockedExecution[+T]
case class Executed[T](value: T) extends LockedExecution[T]
case object Locked extends LockedExecution[Nothing]

trait LockingService {
  def withLock[T](lockId: LockId)(f: => Future[T]): Future[LockedExecution[T]]
}
