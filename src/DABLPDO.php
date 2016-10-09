<?php

/**
 * @link https://github.com/ManifestWebDesign/DABL
 * @link http://manifestwebdesign.com/redmine/projects/dabl
 * @author Manifest Web Design
 * @license    MIT License
 */

namespace Dabl\Adapter;

use Dabl\Adapter\Propel\Model\Database;
use PDO;
use Exception;
use RuntimeException;
use PDOStatement;
use DateTimeZone;

abstract class DABLPDO extends PDO {

	const ID_METHOD_NONE = 0;
	const ID_METHOD_AUTOINCREMENT = 1;
	const ID_METHOD_SEQUENCE = 2;

	public $queryLog = array();
	public static $logQueries = false;
	public static $printQueries = false;

	protected $dbName = null;
	protected $driver = null;

	function getDBName() {
		return $this->dbName;
	}

	function getDriver() {
		return $this->driver;
	}

	function logQuery($query_string, $time) {
		$trace = '';
		$backtrace = debug_backtrace();
		array_shift($backtrace);

		foreach ($backtrace as &$block) {
			$trace .= @ $block['file'] . ' (line ' . @$block['line'] . ') ' . @$block['class'] . @$block['type'] . @$block['function'] . '()<br />';
		}

		$this->queryLog[] = array(
			'query' => $query_string,
			'time' => $time,
			'trace' => $trace
		);
	}

	function __destruct() {
		if (self::$logQueries) {
			$this->printQueryLog();
		}
	}

	/**
	 * @param $connection_params
	 * @return mixed
	 * @deprecated
	 * @see DABLPDO::connect
	 */
	static function factory($connection_params) {
		return self::connect($connection_params);
	}

	/**
	 * Creates a new instance of the database adapter associated
	 * with the specified Creole driver.
	 *
	 */
	static function connect($connection_params) {
		$user = null;
		$password = null;

		if (!empty($connection_params['user'])) {
			$user = $connection_params['user'];
		}

		if (!empty($connection_params['password'])) {
			$password = $connection_params['password'];
		}

		$options = array(
			PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION
		);

		if (
			isset($connection_params['persistant']) &&
			(
				true === $connection_params['persistant']
				|| 1 === $connection_params['persistant']
				|| 'true' === $connection_params['persistant']
				|| '1' === $connection_params['persistant']
				|| 'on' === strtolower($connection_params['persistant'])
			)
		) {
			$options[PDO::ATTR_PERSISTENT] = true;
		}

		$parts = array();

		switch ($connection_params['driver']) {
			case 'access':

//				You may need to download the AccessDatabaseEngine.exe from Microsoft.
//
//				http://www.microsoft.com/en-us/download/details.aspx?id=23734 (2007)
//				http://www.microsoft.com/en-us/download/details.aspx?id=13255 (2010)
//
//				After the download finishes, and installs for your architecture (32 or 64 bit), You will need to create an ODBC connection.
//
//				Instead of creating an ODBC connection through your Administrative Tools, Data Sources (ODBC) interface, you will need to run
//				the 64-bit instance of Data Sources (ODBC). Run "c:\windows\sysWOW64\odbcad32.exe" -- without quotes. Then create the
//				connection to the database as you would on a 32 bit machine.

				if (!empty($connection_params['dbname'])) {
					if (!file_exists($connection_params['dbname'])) {
						throw new \RuntimeException("Could not find database file: {$connection_params['dbname']}");
					}
					$parts[] = 'Dbq=' . addslashes($connection_params['dbname']);
				}
				if (!empty($connection_params['user'])) {
					$parts[] = 'Uid=' . $connection_params['user'];
					$user = null;
				}
				if (!empty($connection_params['password'])) {
					$parts[] = 'Pwd=' . $connection_params['password'];
					$password = null;
				}
				foreach ($parts as &$v) {
					$v = str_replace(';', '\;', $v);
				}
				$dsn = 'odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)};' . implode(';', $parts);
				$class = 'Dabl\\Adapter\\DBAccess';
				break;

			case 'sqlite':
			case 'sqlite2':
				$dsn = $connection_params['driver'] . ':' . $connection_params['dbname'];
				$class = 'Dabl\\Adapter\\DBSQLite';
				break;

			case 'mysql':
				$options[PDO::MYSQL_ATTR_LOCAL_INFILE] = 1;

				if (!empty($connection_params['host'])) {
					$parts[] = 'host=' . $connection_params['host'];
				}
				if (!empty($connection_params['port'])) {
					$parts[] = 'port=' . $connection_params['port'];
				}
				if (!empty($connection_params['unix_socket'])) {
					$parts[] = 'unix_socket=' . $connection_params['unix_socket'];
				}
				if (!empty($connection_params['dbname'])) {
					$parts[] = 'dbname=' . $connection_params['dbname'];
				}
				foreach ($parts as &$v) {
					$v = str_replace(';', '\;', $v);
				}
				$dsn = 'mysql:' . implode(';', $parts);
				$class = 'Dabl\\Adapter\\DBMySQL';
				break;

			case 'oracle':
			case 'oci':
				if (!empty($connection_params['host'])) {
					$dbname = 'dbname=//' . $connection_params['host'];

					if (!empty($connection_params['port'])) {
						$dbname .= ':' . $connection_params['port'];
					}

					if (!empty($connection_params['dbname'])) {
						$dbname .= '/' . $connection_params['dbname'];
					}
					$parts[] = $dbname;
				} elseif (!empty($connection_params['dbname'])) {
					$parts[] = 'dbname=' . $connection_params['dbname'];
				}

				if (!empty($connection_params['charset'])) {
					$parts[] = 'charset=' . $connection_params['charset'];
				}

				foreach ($parts as &$v) {
					$v = str_replace(';', '\;', $v);
				}
				$dsn = 'oci:' . implode(';', $parts);
				$class = 'Dabl\\Adapter\\DBOracle';
				break;

			case 'pgsql':
				if (!empty($connection_params['host'])) {
					$parts[] = 'host=' . $connection_params['host'];
				}
				if (!empty($connection_params['port'])) {
					$parts[] = 'port=' . $connection_params['port'];
				}
				if (!empty($connection_params['dbname'])) {
					$parts[] = 'dbname=' . $connection_params['dbname'];
				}
				if (!empty($connection_params['user'])) {
					$parts[] = 'user=' . $connection_params['user'];
				}
				if (!empty($connection_params['password'])) {
					$parts[] = 'password=' . $connection_params['password'];
				}
				foreach ($parts as &$v) {
					$v = str_replace(' ', '\ ', $v);
				}
				$dsn = 'pgsql:' . implode(' ', $parts);
				$user = null;
				$password = null;
				$class = 'Dabl\\Adapter\\DBPostgres';
				break;
			case 'redshift':
				if (!empty($connection_params['host'])) {
					$parts[] = 'host=' . $connection_params['host'];
				}
				if (!empty($connection_params['port'])) {
					$parts[] = 'port=' . $connection_params['port'];
				}
				if (!empty($connection_params['dbname'])) {
					$parts[] = 'dbname=' . $connection_params['dbname'];
				}
				if (!empty($connection_params['user'])) {
					$parts[] = 'user=' . $connection_params['user'];
				}
				if (!empty($connection_params['password'])) {
					$parts[] = 'password=' . $connection_params['password'];
				}
				foreach ($parts as &$v) {
					$v = str_replace(' ', '\ ', $v);
				}
				$dsn = 'pgsql:' . implode(' ', $parts);
				$user = null;
				$password = null;
				$class = 'Dabl\\Adapter\\DBRedshift';
				break;

			case 'sqlsrv':
				if (!empty($connection_params['host'])) {
					$parts[] = 'server=' . $connection_params['host'];
				}
				if (!empty($connection_params['dbname'])) {
					$parts[] = 'database=' . $connection_params['dbname'];
				}
				if (!empty($connection_params['charset'])) {
					$parts[] = 'charset=' . $connection_params['charset'];
				}
				if (!empty($connection_params['appname'])) {
					$parts[] = 'appname=' . $connection_params['appname'];
				}

				foreach ($parts as &$v) {
					$v = str_replace(';', '\;', $v);
				}
				$dsn = $connection_params['driver'] . ':' . implode(';', $parts);
				$class = 'Dabl\\Adapter\\DBMSSQL';
				break;
			case 'mssql':
			case 'sybase':
			case 'dblib':
				if (!empty($connection_params['host'])) {
					$host = 'host=' . $connection_params['host'];
					if (!empty($connection_params['port'])) {
						$host .= ':' . $connection_params['port'];
					}
					$parts[] = $host;
				}
				if (!empty($connection_params['dbname'])) {
					$parts[] = 'dbname=' . $connection_params['dbname'];
				}
				if (!empty($connection_params['charset'])) {
					$parts[] = 'charset=' . $connection_params['charset'];
				}
				if (!empty($connection_params['appname'])) {
					$parts[] = 'appname=' . $connection_params['appname'];
				}

				// UTF-8 support requires at least tds version 7.0
				if ($connection_params['driver'] === 'dblib') {
					$parts[] = 'version=7.0';
				}

				foreach ($parts as &$v) {
					$v = str_replace(';', '\;', $v);
				}
				$dsn = $connection_params['driver'] . ':' . implode(';', $parts);
				$class = 'Dabl\\Adapter\\DBMSSQL';
				break;

			default:
				throw new RuntimeException("Unsupported database driver: " . $connection_params['driver'] . ": Check your configuration file");
		}

		try {
			$conn = new $class($dsn, $user, $password, $options);
		} catch (Exception $e) {
			throw new RuntimeException($e->getMessage());
		}

		if (!empty($connection_params['dbname'])) {
			$conn->dbName = $connection_params['dbname'];
		}
		$conn->driver = $connection_params['driver'];

		return $conn;
	}

	function __construct() {
		$args = func_get_args();
		$result = call_user_func_array(array('parent', '__construct'), $args);
		if (self::$logQueries || self::$printQueries) {
			$this->setAttribute(PDO::ATTR_STATEMENT_CLASS, array(__NAMESPACE__ . '\\LoggedPDOStatement', array($this)));
		}
		return $result;
	}

	function printQuery($query_string) {
		print_r($query_string);
		echo PHP_EOL . PHP_EOL;
	}

	/**
	 * Executes an SQL statement, returning a result set as a PDOStatement object
	 * @param string $statement The query string to execute as a query
	 * @param int $fetch_mode PDO::FETCH_COLUMN, PDO::FETCH_CLASS, or PDO::FETCH_INTO
	 * @param mixed $mixed column number(int), class name(string), or object($object)
	 * @param array $ctorargs Constructor arguments for PDO::FETCH_CLASS
	 * @return PDOStatement
	 */
	function query($statement, $fetch_mode = null, $mixed = null, array $ctorargs = null) {
		$args = func_get_args();

		if (self::$logQueries) {
			$start = microtime(true);
			$result = call_user_func_array(array('parent', 'query'), $args);
			$time = microtime(true) - $start;
			$this->logQuery((string) $statement, $time);
			return $result;
		}

		if (self::$printQueries) {
			$this->printQuery((string) $statement);
		}

		return call_user_func_array(array('parent', 'query'), $args);
	}

	/**
	 * Execute an SQL statement and return the number of affected rows
	 * @param string $statement The SQL statement to prepare and execute.
	 * @return int
	 */
	function exec($statement) {

		if (self::$logQueries) {
			$start = microtime(true);
			$result = parent::exec($statement);
			$time = microtime(true) - $start;
			$this->logQuery((string) $statement, $time);
			return $result;
		}
		if (self::$printQueries) {
			$this->printQuery((string) $statement);
		}

		return parent::exec($statement);
	}

	function getLoggedQueries() {
		return $this->queryLog;
	}

	function printQueryLog() {
		$total_time = 0.00;
		$total_count = 0;
		$string = '<div style="padding: 10px; background: #fff"><table width="100%" border="1" bordercolor="#bbb" style="clear:both;margin:auto;white-space:pre-line;font-size:11px;font-family:monospace" cellpadding="1" cellspacing="0">';
		$string .= '<thead style="background-color: #eee"><tr><th>Query</th><th>Count</th><th>Time (Seconds)</th><th>Traces</th></tr></thead><tbody>';
		$queries = array();
		foreach ($this->queryLog as $num => &$query_array) {
			$hash = md5(trim($query_array['query']));
			if (!isset($queries[$hash])) {
				$queries[$hash] = array(
					'query' => $query_array['query'],
					'count' => 0,
					'time' => 0.0,
					'traces' => array()
				);
			}
			++$total_count;
			++$queries[$hash]['count'];
			$queries[$hash]['time'] += $query_array['time'];
			$total_time += $query_array['time'];
			$trace_hash = md5(trim($query_array['trace']));
			if (!isset($queries[$hash]['traces'][$trace_hash])) {
				$queries[$hash]['traces'][$trace_hash] = array(
					'trace' => $query_array['trace'],
					'count' => 0,
					'time' => 0.0,
				);
			}
			++$queries[$hash]['traces'][$trace_hash]['count'];
			$queries[$hash]['traces'][$trace_hash]['time'] += $query_array['time'];
		}

		$sort = function($a, $b){
			if ($a['count'] < $b['count']) {
				return 1;
			} elseif ($a['count'] > $b['count']) {
				return -1;
			}
			if ($a['time'] < $b['time']) {
				return 1;
			} elseif ($a['time'] > $b['time']) {
				return -1;
			}
			return 0;
		};
		usort($queries, $sort);

		foreach ($queries as $q) {
			$string .= '<tr><td>' . $q['query'] . '</td><td>' . $q['count'] . '</td><td>' . round($q['time'], 6) . '</td>';
			$string .= '<td width="60%"><table border="0" style="white-space:pre-line;font-size:11px;font-family:monospace" cellpadding="1" cellspacing="0"><thead><tr><th>Trace</th><th>Count</th><th>Time (Seconds)</th></tr></thead><tbody>';
			usort($q['traces'], $sort);
			foreach ($q['traces'] as $trace) {
				$string .= '<tr>';
				$string .= '<tr><td>' . $trace['trace'] . '</td><td>' . $trace['count'] . '</td><td>' . round($trace['time'], 6) . '</td>';
				$string .= '</tr>';
			}
			$string .= '</tbody></table></td></tr>';
		}

		$string .= '<tr><td></td><td nowrap="nowrap">' . $total_count . '</td><td>' . round($total_time, 6) . '</td><td>&nbsp;</td></tr>';
		$string .= '</tbody></table>';
		echo $string;
		echo '<br />' . 'Max Memory Usage: ' . memory_get_peak_usage() / (1024 * 1024) . ' MB</div>';
	}

	/**
	 * This method is called after a connection was created to run necessary
	 * post-initialization queries or code.
	 *
	 * If a charset was specified, this will be set before any other queries
	 * are executed.
	 *
	 * This base method runs queries specified using the "query" setting.
	 *
	 * @param	  array An array of settings.
	 * @see		setCharset()
	 */
	function initConnection(array $settings) {
		if (isset($settings['charset']['value'])) {
			$this->setCharset($settings['charset']['value']);
		}
		if (isset($settings['queries']) && is_array($settings['queries'])) {
			foreach ($settings['queries'] as &$queries) {
				foreach ((array) $queries as $query) {
					$this->exec($query);
				}
			}
		}
	}

	/**
	 * Sets the character encoding using SQL standard SET NAMES statement.
	 *
	 * This method is invoked from the default initConnection() method and must
	 * be overridden for an RDMBS which does _not_ support this SQL standard.
	 *
	 * @param	  string The charset encoding.
	 * @see		initConnection()
	 */
	function setCharset($charset) {
		$this->exec("SET NAMES '" . $charset . "'");
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  string The string to transform to upper case.
	 * @return	 string The upper case string.
	 */
	abstract function toUpperCase($in);

	/**
	 * Returns the character used to indicate the beginning and end of
	 * a piece of text used in a SQL statement (generally a single
	 * quote).
	 *
	 * @return	 string The text delimeter.
	 */
	function getStringDelimiter() {
		return '\'';
	}

	/**
	 * @param mixed $value
	 * @return mixed
	 */
	function prepareInput($value) {
		if (is_array($value)) {
			return array_map(array($this, 'prepareInput'), $value);
		}

		if (is_int($value) || is_float($value)) {
			return $value;
		}

		if (is_bool($value)) {
			return $value ? 1 : 0;
		}

		if ($value === null) {
			return 'NULL';
		}

		return $this->quote($value);
	}

	/**
	 * Deprecated method name.  Use prepareInput()
	 * @deprecated
	 * @param mixed $value
	 * @return mixed
	 */
	function checkInput($value) {
		return $this->prepareInput($value);
	}

	/**
	 * This method is used to ignore case.
	 *
	 * @param	  string $in The string whose case to ignore.
	 * @return	 string The string in a case that can be ignored.
	 */
	abstract function ignoreCase($in);

	/**
	 * This method is used to ignore case in an ORDER BY clause.
	 * Usually it is the same as ignoreCase, but some databases
	 * (Interbase for example) does not use the same SQL in ORDER BY
	 * and other clauses.
	 *
	 * @param	  string $in The string whose case to ignore.
	 * @return	 string The string in a case that can be ignored.
	 */
	function ignoreCaseInOrderBy($in) {
		return $this->ignoreCase($in);
	}

	/**
	 * Returns SQL that converts a date value to the start of the hour
	 *
	 * @param string $date
	 * @return string
	 */
	abstract function hourStart($date);

	/**
	 * Returns SQL that converts a date value to the start of the day
	 *
	 * @param string $date
	 * @return string
	 */
	abstract function dayStart($date);

	/**
	 * Returns SQL that converts a date value to the first day of the week
	 *
	 * @param string $date
	 * @return string
	 */
	abstract function weekStart($date);

	/**
	 * Returns SQL that converts a date value to the first day of the month
	 *
	 * @param string $date
	 * @return string
	 */
	abstract function monthStart($date);

	/**
	 * Returns SQL which converts the date value to its value in the target timezone
	 *
	 * @param string $date SQL column expression
	 * @param string|DateTimeZone $to_tz DateTimeZone or timezone id
	 * @param string|DateTimeZone $from_tz DateTimeZone or timezone id
	 * @return string
	 */
	abstract function convertTimeZone($date, $to_tz, $from_tz = null);

	/**
	 * Returns SQL which concatenates the second string to the first.
	 *
	 * @param	  string String to concatenate.
	 * @param	  string String to append.
	 * @return	 string
	 */
	abstract function concatString($s1, $s2);

	/**
	 * Returns SQL which extracts a substring.
	 *
	 * @param	  string String to extract from.
	 * @param	  int Offset to start from.
	 * @param	  int Number of characters to extract.
	 * @return	 string
	 */
	abstract function subString($s, $pos, $len);

	/**
	 * Returns SQL which calculates the length (in chars) of a string.
	 *
	 * @param	  string String to calculate length of.
	 * @return	 string
	 */
	abstract function strLength($s);

	/**
	 * Quotes database object identifiers (table names, col names, sequences, etc.).
	 * @param	  string|array $text The identifier(s) to quote.
	 * @param    bool $force Quote, even if quotes or spaces are already found.  Has no effect when $text is an array.
	 * @return	 string The quoted identifier.
	 */
	function quoteIdentifier($text, $force = false) {
		if (is_array($text)) {
			return array_map(array($this, 'quoteIdentifier'), $text);
		}

		if (!$force) {
			if (strpos($text, '"') !== false || strpos($text, ' ') !== false || strpos($text, '(') !== false || strpos($text, '*') !== false) {
				return $text;
			}
		}

		return '"' . str_replace('.', '"."', $text) . '"';
	}

	/**
	 * Returns the native ID method for this RDBMS.
	 * @return	 int one of DABLPDO:ID_METHOD_SEQUENCE, DABLPDO::ID_METHOD_AUTOINCREMENT.
	 */
	protected function getIdMethod() {
		return DABLPDO::ID_METHOD_AUTOINCREMENT;
	}

	/**
	 * Whether this adapter uses an ID generation system that requires getting ID _before_ performing INSERT.
	 * @return	 boolean
	 */
	function isGetIdBeforeInsert() {
		return ($this->getIdMethod() === DABLPDO::ID_METHOD_SEQUENCE);
	}

	/**
	 * Whether this adapter uses an ID generation system that requires getting ID _before_ performing INSERT.
	 * @return	 boolean
	 */
	function isGetIdAfterInsert() {
		return ($this->getIdMethod() === DABLPDO::ID_METHOD_AUTOINCREMENT);
	}

	/**
	 * Gets the generated ID (either last ID for autoincrement or next sequence ID).
	 * @return	 mixed
	 */
	function getId($name = null) {
		return $this->lastInsertId($name);
	}

	/**
	 * Returns timestamp formatter string for use in date() function.
	 * @return	 string
	 */
	function getTimestampFormatter() {
		return 'Y-m-d H:i:s';
	}

	/**
	 * Returns date formatter string for use in date() function.
	 * @return	 string
	 */
	function getDateFormatter() {
		return 'Y-m-d';
	}

	/**
	 * Returns time formatter string for use in date() function.
	 * @return	 string
	 */
	function getTimeFormatter() {
		return 'H:i:s';
	}

	/**
	 * Should Column-Names get identifiers for inserts or updates.
	 * By default false is returned -> backwards compability.
	 *
	 * it`s a workaround...!!!
	 *
	 * @todo	   should be abstract
	 * @return	 boolean
	 * @deprecated
	 */
	function useQuoteIdentifier() {
		return false;
	}

	/**
	 * Modifies the passed-in SQL to add LIMIT and/or OFFSET.
	 */
	abstract function applyLimit(&$sql, $offset, $limit);

	/**
	 * Gets the SQL string that this adapter uses for getting a random number.
	 *
	 * @param	  mixed $seed (optional) seed value for databases that support this
	 */
	abstract function random($seed = null);

	/**
	 * @return Database
	 */
	abstract function getDatabaseSchema();

}
