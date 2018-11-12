package de.kaufhof.ets.locking.postgres

import cats.effect.{ContextShift, IO}
import doobie.{ConnectionIO, Fragment, Transactor}
import doobie.implicits._
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

trait DbTest extends WordSpecLike with BeforeAndAfterAll {

  private val driver = "org.postgresql.Driver"
  private val url = "jdbc:postgresql://127.0.0.1:6432/postgres?currentSchema=test"
  private val user = "postgres"
  private val pass = "pass"

  implicit val contextShiftIO: ContextShift[IO] = IO.contextShift(scala.concurrent.ExecutionContext.global)

  val transactor: Transactor[IO] = Transactor.fromDriverManager[IO](driver, url, user, pass)
  val connectionProvider = DriverManager(driver, url, user, pass)

  implicit class CioOps[T](cio: ConnectionIO[T]) {
    def execSync: T = cio.transact(transactor).unsafeRunSync()
  }

  private def setupDb(): Unit = {
    sql"CREATE SCHEMA IF NOT EXISTS test".update.run.execSync
    (fr"DROP TABLE IF EXISTS" ++ Fragment.const(DbTest.tblName)).update.run.execSync
    PGLockingRepository.createTableIfNotExistsSql(DbTest.tblName).run.execSync
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setupDb()
  }
}

object DbTest {
  val tblName = "locking_test"
}
