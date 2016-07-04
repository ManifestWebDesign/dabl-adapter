<?php

use Dabl\Adapter\DABLPDO;
use Dabl\Adapter\DBSQLite;

class DBSQLiteTest extends PHPUnit_Framework_TestCase {

	/**
	 * @var DBSQLite
	 */
	protected $pdo;

	function setUp() {
		$this->pdo = DABLPDO::connect(array(
			'driver' => 'sqlite',
			'dbname' => ':memory:'
		));
		return parent::setUp();
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

		$sql = 'SELECT ' . $this->pdo->weekStart("'2013-12-29'");
		$expected = '2013-12-29';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->weekStart("'2014-01-05'");
		$expected = '2014-01-05';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->weekStart("'2014-07-20'");
		$expected = '2014-07-20';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->weekStart("'2014-07-26 23:59:59'");
		$expected = '2014-07-20';
		$actual = $this->pdo->query($sql)->fetchColumn();
		$this->assertEquals($expected, $actual, $sql . ' should have returned ' . $expected);

		$sql = 'SELECT ' . $this->pdo->weekStart("'2014-07-27 00:00:00'");
		$expected = '2014-07-27';
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
		$this->markTestIncomplete();
	}

	function testSetCharset() {
		$this->markTestIncomplete();
	}

	function testToUpperCase() {
		$this->markTestIncomplete();
	}

	function testIgnoreCase() {
		$this->markTestIncomplete();
	}

	function testConcatString() {
		$this->markTestIncomplete();
	}

	function testSubString() {
		$this->markTestIncomplete();
	}

	function testStrLength() {
		$this->markTestIncomplete();
	}

	function testApplyLimit() {
		$this->markTestIncomplete();
	}

	function testRandom() {
		$this->markTestIncomplete();
	}

	function testGetDatabaseSchema(){
		$this->markTestIncomplete();
	}

}
