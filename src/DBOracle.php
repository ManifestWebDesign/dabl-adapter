<?php

namespace Dabl\Adapter;
use Dabl\Adapter\Propel\Model\Database;
use Dabl\Adapter\Propel\Platform\OraclePlatform;
use Dabl\Adapter\Propel\Reverse\OracleSchemaParser;
use DateTimeZone;
use Exception;
use PDO;
use RuntimeException;

/**
 * Oracle adapter.
 */
class DBOracle extends DABLPDO {

	/**
	 * Returns SQL that converts a date value to the start of the hour
	 *
	 * @param string $date
	 * @return string
	 */
	function hourStart($date) {
		return 'TRUNC(' . $date . ', \'HH\')';
	}

	/**
	 * Returns SQL that converts a date value to the start of the day
	 *
	 * @param string $date
	 * @return string
	 */
	function dayStart($date) {
		return 'TO_DATE(' . $date . ')';
	}

	/**
	 * Returns SQL that converts a date value to the first day of the week
	 *
	 * @param string $date
	 * @return string
	 */
	function weekStart($date) {
		return 'TRUNC(' . $date . ', \'DY\')';
	}

	/**
	 * Returns SQL that converts a date value to the first day of the month
	 *
	 * @param string $date
	 * @return string
	 */
	function monthStart($date) {
		return 'TRUNC(' . $date . ', \'MONTH\')';
	}

	/**
	 * Returns SQL which converts the date value to its value in the target timezone
	 *
	 * @param string $date SQL column expression
	 * @param string|DateTimeZone $to_tz DateTimeZone or timezone id
	 * @param string|DateTimeZone $from_tz DateTimeZone or timezone id
	 * @return string
	 */
	function convertTimeZone($date, $to_tz, $from_tz = null) {
		throw new RuntimeException('Not implemented!');

		if ($to_tz instanceof DateTimeZone) {
			$to_tz = $to_tz->getName();
		}
		if ($from_tz instanceof DateTimeZone) {
			$from_tz = $from_tz->getName();
		}

		return "NEW_TIME($date, '$from_tz', '$to_tz')";
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  string $in The string to transform to upper case.
	 * @return	 string The upper case string.
	 */
	function toUpperCase($in){
		return "UPPER(" . $in . ")";
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  string $in The string whose case to ignore.
	 * @return	 string The string in a case that can be ignored.
	 */
	function ignoreCase($in){
		return "UPPER(" . $in . ")";
	}

	/**
	 * Returns SQL which concatenates the second string to the first.
	 *
	 * @param	  string String to concatenate.
	 * @param	  string String to append.
	 * @return	 string
	 */
	function concatString($s1, $s2){
		return "CONCAT($s1, $s2)";
	}

	/**
	 * Returns SQL which extracts a substring.
	 *
	 * @param	  string String to extract from.
	 * @param	  int Offset to start from.
	 * @param	  int Number of characters to extract.
	 * @return	 string
	 */
	function subString($s, $pos, $len){
		return "SUBSTR($s, $pos, $len)";
	}

	/**
	 * Returns SQL which calculates the length (in chars) of a string.
	 *
	 * @param	  string String to calculate length of.
	 * @return	 string
	 */
	function strLength($s){
		return "LENGTH($s)";
	}

	/**
	 * Returns SQL which limits the result set.
	 *
	 * @param string $sql
	 * @param int $offset
	 * @param int $limit
	 * @see DABLPDO::applyLimit()
	 */
	function applyLimit(&$sql, $offset, $limit) {

		$max = $offset + $limit;

		// nesting all queries, in case there's already a WHERE clause
		$sql = <<<EOF
SELECT A.*, rownum AS PROPEL\$ROWNUM
FROM (
  $sql
) A
WHERE rownum <= $max
EOF;

		if ($offset > 0) {
			$sql = <<<EOF
SELECT B.*
FROM (
  $sql
) B
WHERE B.PROPEL\$ROWNUM > $offset
EOF;
		}
	}

	protected function getIdMethod(){
		return DABLPDO::ID_METHOD_SEQUENCE;
	}

	function getId($name = null){
		if ($name === null) {
			throw new Exception("Unable to fetch next sequence ID without sequence name.");
		}

		$stmt = $this->query("SELECT " . $name . ".nextval FROM dual");
		$row = $stmt->fetch(PDO::FETCH_NUM);

		return $row[0];
	}

	function random($seed=NULL){
		return 'dbms_random.value';
	}

	/**
	 * @return Database
	 */
	function getDatabaseSchema(){
		$parser = new OracleSchemaParser($this);
		$database = new Database($this->getDBName());
		$database->setPlatform(new OraclePlatform($this));
		$parser->parse($database);
		$database->doFinalInitialization();
		return $database;
	}

}
