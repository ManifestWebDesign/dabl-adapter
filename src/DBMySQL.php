<?php

namespace Dabl\Adapter;
use Dabl\Adapter\Propel\Model\Database;
use Dabl\Adapter\Propel\Platform\MysqlPlatform;
use Dabl\Adapter\Propel\Reverse\MysqlSchemaParser;
use DateTimeZone;
use PDOException;

/**
 * This is used in order to connect to a MySQL database.
 */
class DBMySQL extends DABLPDO {

	/**
	 * @var int the current transaction depth
	 */
	protected $_transactionDepth = 0;

	/**
	 * Returns SQL that converts a date value to the start of the hour
	 *
	 * @param string $date
	 * @return string
	 */
	function hourStart($date) {
		return "DATE_FORMAT(" . $date . ", '%Y-%m-%d %H:00:00')";
	}

	/**
	 * Returns SQL that converts a date value to the start of the day
	 *
	 * @param string $date
	 * @return string
	 */
	function dayStart($date) {
		return 'DATE(' . $date . ')';
	}

	/**
	 * Returns SQL that converts a date value to the first day of the week
	 *
	 * @param string $date
	 * @return string
	 */
	function weekStart($date) {
		return 'DATE(DATE_ADD(' . $date . ', INTERVAL(1 - DAYOFWEEK(' . $date . ')) DAY))';
	}

	/**
	 * Returns SQL that converts a date value to the first day of the month
	 *
	 * @param string $date
	 * @return string
	 */
	function monthStart($date) {
		return 'ADDDATE(LAST_DAY(SUBDATE(' . $date . ', INTERVAL 1 MONTH)), 1)';
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
		if ($to_tz instanceof DateTimeZone) {
			$to_tz = $to_tz->getName();
		}
		if ($from_tz instanceof DateTimeZone) {
			$from_tz = $from_tz->getName();
		}
		if (!$from_tz) {
			$from_tz = '@@session.time_zone';
		} else {
			$from_tz = "'$from_tz'";
		}

		return "CONVERT_TZ($date, $from_tz, '$to_tz')";
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  in The string to transform to upper case.
	 * @return	 The upper case string.
	 */
	function toUpperCase($in){
		return "UPPER(" . $in . ")";
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  in The string whose case to ignore.
	 * @return	 The string in a case that can be ignored.
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
		return "SUBSTRING($s, $pos, $len)";
	}

	/**
	 * Returns SQL which calculates the length (in chars) of a string.
	 *
	 * @param	  string String to calculate length of.
	 * @return	 string
	 */
	function strLength($s){
		return "CHAR_LENGTH($s)";
	}


	/**
	 * Locks the specified table.
	 *
	 * @param	  string $table The name of the table to lock.
	 * @throws	 PDOException No Statement could be created or
	 * executed.
	 */
	function lockTable($table){
		$this->exec("LOCK TABLE " . $table . " WRITE");
	}

	/**
	 * Unlocks the specified table.
	 *
	 * @param	  string $table The name of the table to unlock.
	 * @throws	 PDOException No Statement could be created or
	 * executed.
	 */
	function unlockTable($table){
		$this->exec("UNLOCK TABLES");
	}

	/**
	 * @see		DABLPDO::quoteIdentifier()
	 */
	function quoteIdentifier($text, $force = false) {
		if (is_array($text)) {
			return array_map(array($this, 'quoteIdentifier'), $text);
		}

		if (!$force) {
			if (strpos($text, '`') !== false || strpos($text, ' ') !== false || strpos($text, '(') !== false || strpos($text, '*') !== false) {
				return $text;
			}
		}

		return '`' . str_replace('.', '`.`', $text) . '`';
	}

	/**
	 * @see		DABLPDO::useQuoteIdentifier()
	 */
	function useQuoteIdentifier(){
		return true;
	}

	/**
	 * @see		DABLPDO::applyLimit()
	 */
	function applyLimit(&$sql, $offset, $limit){
		if ( $limit > 0 ) {
			$sql .= " LIMIT " . ($offset > 0 ? $offset . ", " : "") . $limit;
		} else if ( $offset > 0 ) {
			$sql .= " LIMIT " . $offset . ", 18446744073709551615";
		}
	}

	/**
	 * @see		DABLPDO::random()
	 */
	function random($seed = null){
		return 'rand('.((int) $seed).')';
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
		$alias = $alias ? " AS " . $this->quoteIdentifier($alias, true) : '';

		return "DATE_FORMAT({$field}, '{$format}'){$alias}";
	}

	/**
	 * Start transaction
	 *
	 * @return bool|void
	 */
	public function beginTransaction() {
		if ($this->_transactionDepth === 0) {
			parent::beginTransaction();
		} else {
			$this->exec("SAVEPOINT LEVEL{$this->_transactionDepth}");
		}

		$this->_transactionDepth++;
	}

	/**
	 * @return int
	 */
	public function getTransactionDepth() {
		return $this->_transactionDepth;
	}

	/**
	 * Commit current transaction
	 *
	 * @return bool|void
	 */
	public function commit() {
		if ($this->_transactionDepth === 0) {
			throw new PDOException('Rollback error : There is no transaction started');
		}

		$this->_transactionDepth--;

		if ($this->_transactionDepth === 0) {
			parent::commit();
		} else {
			$this->exec("RELEASE SAVEPOINT LEVEL{$this->_transactionDepth}");
		}
	}

	/**
	 * Rollback current transaction,
	 *
	 * @throws PDOException if there is no transaction started
	 * @return bool|void
	 */
	public function rollBack() {
		if ($this->_transactionDepth === 0) {
			throw new PDOException('Rollback error : There is no transaction started');
		}

		$this->_transactionDepth--;

		if ($this->_transactionDepth === 0) {
			parent::rollBack();
		} else {
			$this->exec("ROLLBACK TO SAVEPOINT LEVEL{$this->_transactionDepth}");
		}
	}

	/**
	 * @return Database
	 */
	function getDatabaseSchema(){
		$parser = new MysqlSchemaParser($this);
		$database = new Database($this->getDBName());
		$platform = new MysqlPlatform($this);
		$platform->setDefaultTableEngine('InnoDB');
		$database->setPlatform($platform);
		$parser->parse($database);
		$database->doFinalInitialization();
		return $database;
	}

}
