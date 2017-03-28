<?php

namespace Dabl\Adapter;
use Dabl\Adapter\Propel\Model\Database;
use Dabl\Adapter\Propel\Platform\MssqlPlatform;
use Dabl\Adapter\Propel\Reverse\MssqlSchemaParser;
use DateTimeZone;
use InvalidArgumentException;
use RuntimeException;

/**
 * This is used to connect to a MSSQL database.
 */
class DBMSSQL extends DABLPDO {

	/**
	 * Returns SQL that converts a date value to the start of the hour
	 *
	 * @param string $date
	 * @return string
	 */
	function hourStart($date) {
		return "DATEADD(HOUR, DATEDIFF(HOUR, 0, $date), 0)";
	}

	/**
	 * Returns SQL that converts a date value to the start of the day
	 *
	 * @param string $date
	 * @return string
	 */
	function dayStart($date) {
		return "CAST($date AS DATE)";
	}

	/**
	 * Returns SQL that converts a date value to the first day of the week
	 *
	 * @param string $date
	 * @return string
	 */
	function weekStart($date) {
		return "CAST(DATEADD(WEEK, DATEDIFF(WEEK, '19050101', $date), '19050101') AS DATE)";
	}

	/**
	 * Returns SQL that converts a date value to the first day of the month
	 *
	 * @param string $date
	 * @return string
	 */
	function monthStart($date) {
		return "CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, $date), 0) AS DATE)";
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
//		if ($to_tz instanceof DateTimeZone) {
//			$to_tz = $to_tz->getName();
//		}
//		if ($from_tz instanceof DateTimeZone) {
//			$from_tz = $from_tz->getName();
//		}
		throw new RuntimeException('Not implemented!');
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  in The string to transform to upper case.
	 * @return	 The upper case string.
	 */
	function toUpperCase($in) {
		return "UPPER(" . $in . ")";
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  in The string whose case to ignore.
	 * @return	 The string in a case that can be ignored.
	 */
	function ignoreCase($in) {
		return "UPPER(" . $in . ")";
	}

	/**
	 * Returns SQL which concatenates the second string to the first.
	 *
	 * @param	  string String to concatenate.
	 * @param	  string String to append.
	 * @return	 string
	 */
	function concatString($s1, $s2) {
		return "($s1 + $s2)";
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
		return "SUBSTRING($s, $pos, $len)";
	}

	/**
	 * Returns SQL which calculates the length (in chars) of a string.
	 *
	 * @param	  string String to calculate length of.
	 * @return	 string
	 */
	function strLength($s) {
		return "LEN($s)";
	}

	/**
	 * @see		DABLPDO::quoteIdentifier()
	 */
	function quoteIdentifier($text, $force = false) {
		if (is_array($text)) {
			return array_map(array($this, 'quoteIdentifier'), $text);
		}

		if (!$force) {
			if (strpos($text, '[') !== false || strpos($text, ' ') !== false || strpos($text, '(') !== false || strpos($text, '*') !== false) {
				return $text;
			}
		}

		return '[' . str_replace('.', '].[', $text) . ']';
	}

	/**
	 * @see		DABLPDO::random()
	 */
	function random($seed = null) {
		return 'rand(' . ((int) $seed) . ')';
	}

	/**
	 * Convert $field to the format given in $format.
	 *
	 * @see DABLPDO::dateFormat
	 * @param string $field This will *not* be quoted
	 * @param string $format Date format
	 * @param string $alias Alias for the new field - WILL be quoted, if provided
	 * @return string
	 */
	function dateFormat($field, $format, $alias = null) {
		$alias = $alias ? (' AS "' . $this->quoteIdentifier($alias, true) . '"') : '';

		// todo: use strtok() to parse $format
		$parts = array();
		foreach (explode('-', $format) as $part) {
			$expr = false;
			switch (strtolower($part)) {
				case 'yyyy': case 'yy': case '%y':
					$expr = "DATEPART(YY, {$field})";
					break;
				case '%x':
					$expr = "(CASE WHEN DATEPART(ISOWK, {$field}) - DATEPART(WW, {$field}) > 49 THEN -1 ELSE 0 END)+DATEPART(YY, {$field})";
					break;
				case 'ww': case 'w': case '%v':
					$expr = "DATEPART(ISOWK, {$field})";
					break;
				case 'mm': case 'm': case '%m':
					$expr = "DATEPART(MM, {$field})";
					break;
				case 'dd': case 'd': case '%d':
					$expr = "DATEPART(DD, {$field})";
					break;
				default:
					$expr = "DATEPART({$part}, {$field})";
					break;
			}
			if ($expr) {
				$expr = "CAST({$expr} AS VARCHAR)";
				$length = false;

				switch ($part) {
					case 'YYYY': case 'yyyy': case '%Y':
						$length = 4;
						break;
					case 'YY': case 'yy': case '%y':
					case '%d': case 'DD': case 'dd':
					case '%m': case 'MM': case 'mm':
						$length = 2;
						break;
				}

				if ($length) {
					$expr = "RIGHT('" . str_repeat('0', $length) . "' + {$expr}, {$length})";
				}

				$parts[] = $expr;
			}
		}

		foreach ($parts as &$v)
			$v = "CAST({$v} AS VARCHAR)";
		return join("+ '-' +", $parts) . $alias;
	}

	/**
	 * Simulated Limit/Offset
	 *
	 * This rewrites the $sql query to apply the offset and limit.
	 * some of the ORDER BY logic borrowed from Doctrine MsSqlPlatform
	 *
	 * @see       AdapterInterface::applyLimit()
	 * @author    Benjamin Runnels <kraven@kraven.org>
	 *
	 * @param     string   $sql
	 * @param     integer  $offset
	 * @param     integer  $limit
	 *
	 * @return    void
	 */
	public function applyLimit(&$sql, $offset, $limit) {
		// make sure offset and limit are numeric
		if (!is_numeric($offset) || !is_numeric($limit)) {
			throw new InvalidArgumentException('MssqlAdapter::applyLimit() expects a number for argument 2 and 3');
		}

		//split the select and from clauses out of the original query
		$selectSegment = array();

		$selectText = 'SELECT ';

		preg_match('/\Aselect(.*)from(.*)/si', $sql, $selectSegment);
		if (count($selectSegment) == 3) {
			$selectStatement = trim($selectSegment[1]);
			$fromStatement = trim($selectSegment[2]);
		} else {
			throw new RuntimeException('MssqlAdapter::applyLimit() could not locate the select statement at the start of the query.');
		}

		if (preg_match('/\Aselect(\s+)distinct/i', $sql)) {
			$selectText .= 'DISTINCT ';
			$selectStatement = str_ireplace('distinct ', '', $selectStatement);
		}

		// if we're starting at offset 0 then theres no need to simulate limit,
		// just grab the top $limit number of rows
		if ($offset == 0) {
			$sql = $selectText . 'TOP ' . $limit . ' ' . $selectStatement . ' FROM ' . $fromStatement;

			return;
		}

		// get the ORDER BY clause if present
		$orderStatement = stristr($sql, 'ORDER BY');

		if ($orderStatement === false) {
			$sql = "$sql ORDER BY (SELECT 1)";
		}

		if ($offset != 0) {
			$sql = "$sql OFFSET $offset ROWS";
		}

		if ($limit != 0) {
			$sql = "$sql FETCH NEXT $limit ROWS ONLY";
		}

	}

	/**
	 * @return Database
	 */
	function getDatabaseSchema() {
		$parser = new MssqlSchemaParser($this);
		$database = new Database($this->getDBName());
		$database->setPlatform(new MssqlPlatform($this));
		$parser->parse($database);
		$database->doFinalInitialization();
		return $database;
	}

//	function beginTransaction() {
//		$this->query('BEGIN TRANSACTION');
//	}
//
//	function commit() {
//		$this->query('COMMIT TRANSACTION');
//	}
//
//	function rollback() {
//		$this->query('ROLLBACK TRANSACTION');
//	}

	public function prepareInput($value) {
		if (
			is_string($value)
			&& function_exists('mb_detect_encoding')
			&& mb_detect_encoding($value) === 'UTF-8'
		) {
			return 'N' . parent::prepareInput($value);
		}

		return parent::prepareInput($value);
	}

}