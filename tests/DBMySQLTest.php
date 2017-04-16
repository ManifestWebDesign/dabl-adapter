<?php

use Dabl\Adapter\DABLPDO;
use Dabl\Adapter\DBMySQL;
use Dabl\Adapter\Propel\Model\Database;

class DBMySQLTest extends PHPUnit_Framework_TestCase {

	/**
	 * @var DBMySQL
	 */
	protected $pdo;

	function setUp() {
		try {
			$this->pdo = DABLPDO::connect(array(
				'driver' => 'mysql',
				'host' => 'localhost',
				'dbname' => 'test',
				'user' => 'root',
				'password' => ''
			));
		} catch (Exception $e) {
			$this->markTestSkipped('Unable to connect to Postgres test database' + $e->getMessage());
		}
		return parent::setUp();
	}

	function testGetDatabaseSchema() {
		$database = $this->pdo->getDatabaseSchema();
		$this->assertTrue($database instanceof Database);
	}

	function testHourStart() {
		$sql = 'SELECT ' . $this->pdo->hourStart("'2014-05-05 10:05:15'");
		$expected = '2014-05-05 10:00:00';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->hourStart("'bad date'");
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertNull($actual, $sql . ' should have returned null');
	}

	function testDayStart() {
		$sql = 'SELECT ' . $this->pdo->dayStart("'2014-05-05 10:05:15'");
		$expected = '2014-05-05';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->dayStart("'bad date'");
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertNull($actual, $sql . ' should have returned null');
	}

	function testWeekStart() {
		$sql = 'SELECT ' . $this->pdo->weekStart("'2014-07-25 15:01:19'");
		$expected = '2014-07-20';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->weekStart("'bad date'");
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertNull($actual, $sql . ' should have returned null');
	}

	function testMonthStart() {
		$sql = 'SELECT ' . $this->pdo->monthStart("'2014-07-25 15:01:19'");
		$expected = '2014-07-01';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->monthStart("'bad date'");
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertNull($actual, $sql . ' should have returned null');
	}

	function testConvertTimeZone() {
		$this->pdo->exec("SET time_zone = 'UTC'");
		$sql = 'SELECT ' . $this->pdo->convertTimeZone("'2014-07-25 15:01:19'", 'America/Los_Angeles');
		$expected = '2014-07-25 08:01:19';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$this->pdo->exec("SET time_zone = 'America/Chicago'");
		$sql = 'SELECT ' . $this->pdo->convertTimeZone("'2014-07-25 15:01:19'", 'America/Los_Angeles');
		$expected = '2014-07-25 13:01:19';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$this->pdo->exec("SET time_zone = 'UTC'");
		$sql = 'SELECT ' . $this->pdo->convertTimeZone("'2014-07-25 15:01:19'", 'America/Los_Angeles', 'America/Chicago');
		$expected = '2014-07-25 13:01:19';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);
	}

	/**
	 * @covers DBMySQL::transact
	 */
	function testCallbackInTransact() {
		$this->assertEquals(0, $this->pdo->getTransactionDepth());
		$this->pdo->transact(function() {
			$this->assertEquals(1, $this->pdo->getTransactionDepth());
		});
		$this->assertEquals(0, $this->pdo->getTransactionDepth());
	}

	/**
	 * @covers DBMySQL::transact
	 * @expectedException RuntimeException
	 * @expectedExceptionMessage Foobar
	 */
	function testExceptionInCallbackInTransact() {
		$this->pdo->transact(function() {
			throw new RuntimeException('Foobar');
		});
	}

	/**
	 * @group NestedTransaction
	 * @group bug1355
	 * @covers DBMySQL::beginTransaction
	 */
	function testBeginTransaction() {
		$this->pdo->beginTransaction();
		$this->assertEquals(1, $this->pdo->getTransactionDepth());
		$this->pdo->rollback();
	}

	/**
	 * @group NestedTransaction
	 * @group bug1355
	 * @covers DBMySQL::rollback
	 * @expectedException PDOException
	 */
	function testRollbackOutsideTransaction() {
		$this->pdo->rollback();
	}

	/**
	 * @group NestedTransaction
	 * @group bug1355
	 * @covers DBMySQL::commit
	 * @expectedException PDOException
	 */
	function testCommitOutsideTransaction() {
		$this->pdo->commit();
	}

	/**
	 * @group NestedTransaction
	 * @group bug1355
	 * @covers DBMySQL::rollback
	 */
	function testNestedRollback() {
		$this->pdo->beginTransaction();
		$this->pdo->beginTransaction();
		$this->pdo->rollback();
		$this->pdo->rollback();
		$this->assertTrue(true);
	}

	/**
	 * @group NestedTransaction
	 * @group bug1355
	 * @covers DBMySQL::commit
	 */
	function testNestedCommit() {
		$this->pdo->beginTransaction();
		$this->pdo->beginTransaction();
		$this->pdo->commit();
		$this->assertEquals(1, $this->pdo->getTransactionDepth());
		$this->pdo->commit();
	}

	/**
	 * @group NestedTransaction
	 * @group bug1355
	 * @covers DBMySQL::commit
	 */
	function testRollbackBeforeCommit() {
		$this->pdo->beginTransaction();
		$this->pdo->beginTransaction();
		$this->pdo->rollback();
		$this->pdo->commit();
		$this->assertTrue(true);
	}
}
