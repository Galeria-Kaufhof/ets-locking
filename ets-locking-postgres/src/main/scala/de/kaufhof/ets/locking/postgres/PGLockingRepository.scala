package de.kaufhof.ets.locking.postgres

import java.time.{Clock, Instant}

import cats.data.NonEmptyList
import cats.free.Free
import cats.implicits._
import doobie._
import doobie.implicits._
import de.kaufhof.ets.locking.core.LockId

case class LockInstanceId(v: String)

private[postgres] class PGLockingRepository(tableName: String)(implicit clock: Clock) {
  import PGLockingRepository._

  def getLockInfo(lockId: LockId): ConnectionIO[Option[(LockInstanceId, Instant)]] = {
    PGLockingRepository.findLock(tableName, lockId)
      .option
      .map(_.map(row => (row.instanceId, Instant.ofEpochMilli(row.lockValidUntil))))
  }

  def acquireLock(lockId: LockId, instanceId: LockInstanceId, validUntil: Instant): ConnectionIO[Boolean] =
    for {
      _      <- setIsolationLevelSerializable
      locked <- PGLockingRepository.upsertLock(tableName, lockId, instanceId, validUntil).run.map(_ == 1)
    } yield locked

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def refreshLocks(locks: Set[LockId], instanceId: LockInstanceId, validUntil: Instant): ConnectionIO[Set[LockId]] = {
    NonEmptyList.fromList(locks.toList) match {
      case Some(nel) =>
        for {
          _      <- setIsolationLevelSerializable
          locks  <- nel.map(lockId => PGLockingRepository.upsertLock(tableName, lockId, instanceId, validUntil).run.map(affectedRows => (lockId, affectedRows == 1))).sequence
        } yield locks.filter(_._2 == true).map(_._1).toList.toSet
      case None =>
        Free.pure(Set.empty[LockId])
    }
  }

  def releaseLock(lockId: LockId, instanceId: LockInstanceId): ConnectionIO[Boolean] =
    for {
      _       <- setIsolationLevelSerializable
      deleted <- PGLockingRepository.deleteLock(tableName, lockId, instanceId).run.map(_ == 1)
    } yield deleted

  def createTableIfNotExists: ConnectionIO[Unit] =
    createTableIfNotExistsSql(tableName).run.map(_ => ())

}

case class LockRow(lockId: LockId, instanceId: LockInstanceId, lockValidUntil: Long)

private[postgres] object PGLockingRepository {

  val setIsolationLevelSerializable: ConnectionIO[Unit] =
    sql"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE".update.run.map(_ => ())

  def findLock(tblName: String, lockId: LockId): Query0[LockRow] =
    (fr"""
      SELECT
        lock_id,
        instance_id,
        valid_until
      FROM
        """ ++ Fragment.const(tblName) ++ fr"""
      WHERE
        lock_id = $lockId
    """).query[LockRow]

  def upsertLock(tblName: String, lockId: LockId, instanceId: LockInstanceId, validUntil: Instant)(implicit clock: Clock): Update0 = {
    val validUntilTs = validUntil.toEpochMilli
    val updateTs = clock.instant().toEpochMilli

    (fr"""
      INSERT INTO
        """ ++ Fragment.const(tblName) ++ fr""" (lock_id, instance_id, valid_until)
      VALUES
        ($lockId, $instanceId, $validUntilTs)
      ON CONFLICT (lock_id)
      DO UPDATE SET
        instance_id = $instanceId,
        valid_until = $validUntilTs
      WHERE""" ++
        Fragment.const(tblName + ".instance_id") ++ fr""" = $instanceId
        OR""" ++ Fragment.const(tblName + ".valid_until") ++ fr""" < $updateTs
    """).update
  }

  def deleteLock(tblName: String, lockId: LockId, instanceId: LockInstanceId)(implicit clock: Clock): Update0 = {
    val updateTs = clock.instant().toEpochMilli

    (fr"""
      DELETE FROM
        """ ++ Fragment.const(tblName) ++ fr"""
      WHERE
        lock_id = $lockId
        AND (instance_id = $instanceId OR valid_until < $updateTs)
    """).update
  }

  def createTableIfNotExistsSql(tblName: String): Update0 =
    (fr"CREATE TABLE" ++ Fragment.const(tblName) ++ fr"""(
        lock_id text NOT NULL PRIMARY KEY,
        instance_id text NOT NULL,
        valid_until bigint NOT NULL
      )
    """).update

}
