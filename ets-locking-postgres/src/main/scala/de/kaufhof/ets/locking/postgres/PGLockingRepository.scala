package de.kaufhof.ets.locking.postgres

import java.time.{Clock, Instant}

import cats.data.NonEmptyList
import cats.free.Free
import cats.implicits._
import doobie._
import doobie.implicits._

import de.kaufhof.ets.locking.core.LockId

case class LockInstanceId(v: String)

class PGLockingRepository(tableName: String)(implicit clock: Clock) {
  import PGLockingRepository._

  private val tableNameFragment = Fragment.const(tableName)

  def getLockInfo(lockId: LockId): ConnectionIO[Option[(LockInstanceId, Instant)]] = {
    PGLockingRepository.findLock(tableNameFragment, lockId)
      .option
      .map(_.map(row => (row.instanceId, Instant.ofEpochMilli(row.lockValidUntil))))
  }

  def acquireLock(lockId: LockId, instanceId: LockInstanceId, validUntil: Instant): ConnectionIO[Boolean] =
    for {
      _      <- setIsolationLevelSerializable
      locked <- PGLockingRepository.upsertLock(tableNameFragment, lockId, instanceId, validUntil).run.map(_ == 1)
    } yield locked

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def refreshLocks(locks: Set[LockId], instanceId: LockInstanceId, validUntil: Instant): ConnectionIO[Set[LockId]] = {
    NonEmptyList.fromList(locks.toList) match {
      case Some(nel) =>
        for {
          _      <- setIsolationLevelSerializable
          locks  <- nel.map(lockId => PGLockingRepository.upsertLock(tableNameFragment, lockId, instanceId, validUntil).run.map(affectedRows => (lockId, affectedRows == 1))).sequence
        } yield locks.filter(_._2 == true).map(_._1).toList.toSet
      case None =>
        Free.pure(Set.empty[LockId])
    }
  }

  def releaseLock(lockId: LockId, instanceId: LockInstanceId): ConnectionIO[Boolean] =
    for {
      _       <- setIsolationLevelSerializable
      deleted <- PGLockingRepository.deleteLock(tableNameFragment, lockId, instanceId).run.map(_ == 1)
    } yield deleted

}

case class LockRow(lockId: LockId, instanceId: LockInstanceId, lockValidUntil: Long)

object PGLockingRepository {

  val setIsolationLevelSerializable: ConnectionIO[Unit] =
    sql"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE".update.run.map(_ => ())

  def findLock(tblName: Fragment, lockId: LockId): Query0[LockRow] =
    (fr"""
      SELECT
        lock_id,
        instance_id,
        valid_until
      FROM
        """ ++ tblName ++ fr"""
      WHERE
        lock_id = $lockId
    """).query[LockRow]

  def upsertLock(tblName: Fragment, lockId: LockId, instanceId: LockInstanceId, validUntil: Instant)(implicit clock: Clock): Update0 = {
    val validUntilTs = validUntil.toEpochMilli
    val updateTs = clock.instant().toEpochMilli

    (fr"""
      INSERT INTO
        """ ++ tblName ++ fr""" (lock_id, instance_id, valid_until)
      VALUES
        ($lockId, $instanceId, $validUntilTs)
      ON CONFLICT (lock_id)
      DO UPDATE SET
        instance_id = $instanceId,
        valid_until = $validUntilTs
      WHERE
        pim_locking.instance_id = $instanceId
        OR pim_locking.valid_until < $updateTs
    """).update
  }

  def deleteLock(tblName: Fragment, lockId: LockId, instanceId: LockInstanceId)(implicit clock: Clock): Update0 = {
    val updateTs = clock.instant().toEpochMilli

    (fr"""
      DELETE FROM
        """ ++ tblName ++ fr"""
      WHERE
        lock_id = $lockId
        AND (instance_id = $instanceId OR valid_until < $updateTs)
    """).update
  }

}
