<?php

namespace Dabl\Adapter;
use Dabl\Adapter\Propel\Model\Database;
use Dabl\Adapter\Propel\Platform\SqlitePlatform;
use Dabl\Adapter\Propel\Reverse\SqliteSchemaParser;
use DateTimeZone;

/**
 * This is used in order to connect to a SQLite database.
 */
class DBSQLite extends DABLPDO {

	/**
	 * Returns SQL that converts a date value to the start of the hour
	 *
	 * @param string $date
	 * @return string
	 */
	function hourStart($date) {
		return "datetime($date, '-' || STRFTIME('%M', $date) || ' minutes', '-' || STRFTIME('%S', $date) || ' seconds')";
	}

	/**
	 * Returns SQL that converts a date value to the start of the day
	 *
	 * @param string $date
	 * @return string
	 */
	function dayStart($date) {
		return "DATE($date, 'start of day')";
	}

	/**
	 * Returns SQL that converts a date value to the first day of the week
	 *
	 * @param string $date
	 * @return string
	 */
	function weekStart($date) {
		return "CASE DATE($date, 'weekday 0') "
				. "WHEN DATE($date) THEN DATE($date) "
				. "ELSE DATE($date, 'weekday 0', '-7 days') "
				. "END";
	}

	/**
	 * Returns SQL that converts a date value to the first day of the month
	 *
	 * @param string $date
	 * @return string
	 */
	function monthStart($date) {
		return "DATE($date, 'start of month')";
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
		return "DATETIME($date)";
//		if ($to_tz instanceof DateTimeZone) {
//			$to_tz = $to_tz->getName();
//		}
//		if ($from_tz instanceof DateTimeZone) {
//			$from_tz = $from_tz->getName();
//		}
//		return "DATETIME($date, '$to_tz')";
	}

	/**
	 * For SQLite this method has no effect, since SQLite doesn't support specifying a character
	 * set (or, another way to look at it, it doesn't require a single character set per DB).
	 *
	 * @param	  string The charset encoding.
	 * @throws	 Exception If the specified charset doesn't match sqlite_libencoding()
	 */
	function setCharset($charset) {
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  in The string to transform to upper case.
	 * @return	 The upper case string.
	 */
	function toUpperCase($in) {
		return 'UPPER(' . $in . ')';
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  in The string whose case to ignore.
	 * @return	 The string in a case that can be ignored.
	 */
	function ignoreCase($in) {
		return 'UPPER(' . $in . ')';
	}

	/**
	 * Returns SQL which concatenates the second string to the first.
	 *
	 * @param	  string String to concatenate.
	 * @param	  string String to append.
	 * @return	 string
	 */
	function concatString($s1, $s2) {
		return "($s1 || $s2)";
	}

	/**
	 * Returns SQL which extracts a substring.
	 *
	 * @param	  string String to extract from.
	 * @param	  int Offset to start from.
	 * @param	  int Number of characters to extract.
	 * @return	 string
	 */
	function subString($s, $pos, $len) {
		return "substr($s, $pos, $len)";
	}

	/**
	 * Returns SQL which calculates the length (in chars) of a string.
	 *
	 * @param	  string String to calculate length of.
	 * @return	 string
	 */
	function strLength($s) {
		return "length($s)";
	}

	/**
	 * @see		DABLPDO::applyLimit()
	 */
	function applyLimit(&$sql, $offset, $limit) {
		if ( $limit > 0 ) {
			$sql .= " LIMIT " . $limit . ($offset > 0 ? " OFFSET " . $offset : "");
		} elseif ( $offset > 0 ) {
			$sql .= " LIMIT -1 OFFSET " . $offset;
		}
	}

	function random($seed=NULL) {
		return 'random()';
	}

	/**
	 * @return Database
	 */
	function getDatabaseSchema() {
		$parser = new SqliteSchemaParser($this);
		$database = new Database($this->getDBName());
		$database->setPlatform(new SqlitePlatform($this));
		$parser->parse($database);
		$database->doFinalInitialization();
		return $database;
	}

}
