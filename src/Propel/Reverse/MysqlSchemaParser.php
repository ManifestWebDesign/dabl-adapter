<?php

/**
 * This file is part of the Propel package.
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 * @license    MIT License
 */

namespace Dabl\Adapter\Propel\Reverse;
use Dabl\Adapter\Propel\Model\Column;
use Dabl\Adapter\Propel\Model\ColumnDefaultValue;
use Dabl\Adapter\Propel\Model\Database;
use Dabl\Adapter\Propel\Model\ForeignKey;
use Dabl\Adapter\Propel\Model\Index;
use Dabl\Adapter\Propel\Model\PropelTypes;
use Dabl\Adapter\Propel\Model\Table;
use Dabl\Adapter\Propel\Model\Unique;
use PDO;

/**
 * Mysql database schema parser.
 *
 * @author     Hans Lellelid <hans@xmpl.org>
 * @version    $Revision: 2090 $
 * @package    propel.generator.reverse.mysql
 */
class MysqlSchemaParser extends BaseSchemaParser
{

	/**
	 * @var        boolean
	 */
	private $addVendorInfo = false;

	/**
	 * Map MySQL native types to Propel types.
	 * @var        array
	 */
	private static $mysqlTypeMap = array(
		'tinyint' => PropelTypes::TINYINT,
		'smallint' => PropelTypes::SMALLINT,
		'mediumint' => PropelTypes::SMALLINT,
		'int' => PropelTypes::INTEGER,
		'integer' => PropelTypes::INTEGER,
		'bigint' => PropelTypes::BIGINT,
		'int24' => PropelTypes::BIGINT,
		'real' => PropelTypes::REAL,
		'float' => PropelTypes::FLOAT,
		'decimal' => PropelTypes::DECIMAL,
		'numeric' => PropelTypes::NUMERIC,
		'double' => PropelTypes::DOUBLE,
		'char' => PropelTypes::CHAR,
		'varchar' => PropelTypes::VARCHAR,
		'date' => PropelTypes::DATE,
		'time' => PropelTypes::TIME,
		'year' => PropelTypes::INTEGER,
		'datetime' => PropelTypes::TIMESTAMP,
		'timestamp' => PropelTypes::TIMESTAMP,
		'tinyblob' => PropelTypes::BINARY,
		'blob' => PropelTypes::BLOB,
		'mediumblob' => PropelTypes::BLOB,
		'longblob' => PropelTypes::BLOB,
		'longtext' => PropelTypes::CLOB,
		'tinytext' => PropelTypes::VARCHAR,
		'mediumtext' => PropelTypes::LONGVARCHAR,
		'text' => PropelTypes::LONGVARCHAR,
		'enum' => PropelTypes::CHAR,
		'set' => PropelTypes::CHAR,
		'bit' => PropelTypes::BOOLEAN,
	);

	protected static $defaultTypeSizes = array(
		'char'     => 1,
		'tinyint'  => 4,
		'smallint' => 6,
		'int'      => 11,
		'bigint'   => 20,
		'decimal'  => 10,
	);

	/**
	 * Gets a type mapping from native types to Propel types
	 *
	 * @return     array
	 */
	protected function getTypeMapping()
	{
		return self::$mysqlTypeMap;
	}

	/**
	 *
	 */
	public function parse(Database $database, Task $task = null)
	{
//		$this->addVendorInfo = $this->getGeneratorConfig()->getBuildProperty('addVendorInfo');

		$stmt = $this->dbh->query("SHOW TABLES");

		// First load the tables (important that this happen before filling out details of tables)
		$tables = array();
		if ($task) $task->log("Reverse Engineering Tables", Project::MSG_VERBOSE);
		while ($row = $stmt->fetch(PDO::FETCH_NUM)) {
			$name = $row[0];
			if ($name == $this->getMigrationTable()) {
				continue;
			}
			if ($task) $task->log("  Adding table '" . $name . "'", Project::MSG_VERBOSE);
			$table = new Table($name);
			$database->addTable($table);
			$tables[] = $table;
		}

		// Now populate only columns.
		if ($task) $task->log("Reverse Engineering Columns", Project::MSG_VERBOSE);
		foreach ($tables as $table) {
			if ($task) $task->log("  Adding columns for table '" . $table->getName() . "'", Project::MSG_VERBOSE);
			$this->addColumns($table);
		}

		// Now add indices and constraints.
		if ($task) $task->log("Reverse Engineering Indices And Constraints", Project::MSG_VERBOSE);
		foreach ($tables as $table) {
			if ($task) $task->log("  Adding indices and constraints for table '" . $table->getName() . "'", Project::MSG_VERBOSE);
			$this->addForeignKeys($table);
			$this->addIndexes($table);
			$this->addPrimaryKey($table);
			if ($this->addVendorInfo) {
				$this->addTableVendorInfo($table);
			}
		}

		return count($tables);
	}


	/**
	 * Adds Columns to the specified table.
	 *
	 * @param      Table $table The Table model class to add columns to.
	 */
	protected function addColumns(Table $table)
	{
		$stmt = $this->dbh->query("SHOW COLUMNS FROM `" . $table->getName() . "`");
		$table_quoted = $this->dbh->quote($table->getName());
		$db_quoted = $this->dbh->quote($table->getDatabase()->getName());

		while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			$field_quoted = $this->dbh->quote($row['Field']);

			$row['Comment'] = $this->dbh->query("
				SELECT
				COLUMN_COMMENT
				FROM information_schema.COLUMNS
				WHERE TABLE_NAME=$table_quoted
					AND TABLE_SCHEMA=$db_quoted
					AND COLUMN_NAME=$field_quoted LIMIT 1")->fetchColumn();

			$name = $row['Field'];
			$is_nullable = ($row['Null'] == 'YES');
			$autoincrement = (strpos($row['Extra'], 'auto_increment') !== false);
			$size = null;
			$precision = null;
			$scale = null;

			if (preg_match('/^(\w+)[\(]?([\d,]*)[\)]?( |$)/', $row['Type'], $matches)) {
				//            colname[1]   size/precision[2]
				$nativeType = $matches[1];
				if ($matches[2]) {
					if ( ($cpos = strpos($matches[2], ',')) !== false) {
						$size = (int) substr($matches[2], 0, $cpos);
						$precision = $size;
						$scale = (int) substr($matches[2], $cpos + 1);
					} else {
						$size = (int) $matches[2];
					}
				}
				foreach (self::$defaultTypeSizes as $type => $defaultSize) {
					if ($nativeType == $type && $size == $defaultSize) {
						$size = null;
						continue;
					}
				}
			} elseif (preg_match('/^(\w+)\(/', $row['Type'], $matches)) {
				$nativeType = $matches[1];
			} else {
				$nativeType = $row['Type'];
			}

			//BLOBs can't have any default values in MySQL
			$default = preg_match('~blob|text~', $nativeType) ? null : $row['Default'];

			$propelType = $this->getMappedPropelType($nativeType);

			if (
				in_array($propelType, array(PropelTypes::INTEGER, PropelTypes::BIGINT))
				&& strpos($row['Comment'], 'timestamp') === 0
			) {
				$propelType = PropelTypes::INTEGER_TIMESTAMP;
			} elseif (!$propelType) {
				$propelType = Column::DEFAULT_TYPE;
				$this->warn("Column [" . $table->getName() . "." . $name. "] has a column type (".$nativeType.") that Propel does not support.");
			}

			$column = new Column($name);
			$column->setTable($table);
			$column->setDomainForType($propelType);
			// We may want to provide an option to include this:
			// $column->getDomain()->replaceSqlType($type);
			$column->getDomain()->replaceSize($size);
			$column->getDomain()->replaceScale($scale);
			if ($default !== null) {
				if ($propelType == PropelTypes::BOOLEAN) {
					if ($default == '1') $default = 'true';
					if ($default == '0') $default = 'false';
				}
				if (in_array($default, array('CURRENT_TIMESTAMP'))) {
					$type = ColumnDefaultValue::TYPE_EXPR;
				} else {
					$type = ColumnDefaultValue::TYPE_VALUE;
				}
				$column->getDomain()->setDefaultValue(new ColumnDefaultValue($default, $type));
			}
			$column->setAutoIncrement($autoincrement);
			$column->setNotNull(!$is_nullable);

			if ($this->addVendorInfo) {
				$vi = $this->getNewVendorInfoObject($row);
				$column->addVendorInfo($vi);
			}

			$table->addColumn($column);
		}


	} // addColumn()

	/**
	 * Load foreign keys for this table.
	 */
	protected function addForeignKeys(Table $table)
	{
		$database = $table->getDatabase();

		$stmt = $this->dbh->query("SHOW CREATE TABLE `" . $table->getName(). "`");
		$row = $stmt->fetch(PDO::FETCH_NUM);

		$foreignKeys = array(); // local store to avoid duplicates

		// Get the information on all the foreign keys
		$regEx = '/CONSTRAINT `([^`]+)` FOREIGN KEY \((.+)\) REFERENCES `([^`]*)` \((.+)\)(.*)/';
		if (preg_match_all($regEx,$row[1],$matches)) {
			$tmpArray = array_keys($matches[0]);
			foreach ($tmpArray as $curKey) {
				$name = $matches[1][$curKey];
				$rawlcol = $matches[2][$curKey];
				$ftbl = $matches[3][$curKey];
				$rawfcol = $matches[4][$curKey];
				$fkey = $matches[5][$curKey];

				$lcols = array();
				foreach(preg_split('/`, `/', $rawlcol) as $piece) {
					$lcols[] = trim($piece, '` ');
				}

				$fcols = array();
				foreach(preg_split('/`, `/', $rawfcol) as $piece) {
					$fcols[] = trim($piece, '` ');
				}

				//typical for mysql is RESTRICT
				$fkactions = array(
					'ON DELETE'	=> ForeignKey::RESTRICT,
					'ON UPDATE'	=> ForeignKey::RESTRICT,
				);

				if ($fkey) {
					//split foreign key information -> search for ON DELETE and afterwords for ON UPDATE action
					foreach (array_keys($fkactions) as $fkaction) {
						$result = NULL;
						preg_match('/' . $fkaction . ' (' . ForeignKey::CASCADE . '|' . ForeignKey::SETNULL . ')/', $fkey, $result);
						if ($result && is_array($result) && isset($result[1])) {
							$fkactions[$fkaction] = $result[1];
						}
					}
				}

				// restrict is the default
				foreach ($fkactions as $key => $action) {
					if ($action == ForeignKey::RESTRICT) {
						$fkactions[$key] = null;
					}
				}

				$localColumns = array();
				$foreignColumns = array();
				;
				$foreignTable = $database->getTable($ftbl, true);

				foreach($fcols as $fcol) {
					$foreignColumns[] = $foreignTable->getColumn($fcol);
				}
				foreach($lcols as $lcol) {
					$localColumns[] = $table->getColumn($lcol);
				}

				if (!isset($foreignKeys[$name])) {
					$fk = new ForeignKey($name);
					$fk->setForeignTableCommonName($foreignTable->getCommonName());
					$fk->setForeignSchemaName($foreignTable->getSchema());
					$fk->setOnDelete($fkactions['ON DELETE']);
					$fk->setOnUpdate($fkactions['ON UPDATE']);
					$table->addForeignKey($fk);
					$foreignKeys[$name] = $fk;
				}

				for($i=0; $i < count($localColumns); $i++) {
					$foreignKeys[$name]->addReference($localColumns[$i], $foreignColumns[$i]);
				}

			}

		}

	}

	/**
	 * Load indexes for this table
	 */
	protected function addIndexes(Table $table)
	{
		$stmt = $this->dbh->query("SHOW INDEX FROM `" . $table->getName() . "`");

		// Loop through the returned results, grouping the same key_name together
		// adding each column for that key.

		$indexes = array();
		while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			$colName = $row["Column_name"];
			$name = $row["Key_name"];

			if ($name == "PRIMARY") {
				continue;
			}

			if (!isset($indexes[$name])) {
				$isUnique = ($row["Non_unique"] == 0);
				if ($isUnique) {
					$indexes[$name] = new Unique($name);
				} else {
					$indexes[$name] = new Index($name);
				}
				if ($this->addVendorInfo) {
					$vi = $this->getNewVendorInfoObject($row);
					$indexes[$name]->addVendorInfo($vi);
				}
				$table->addIndex($indexes[$name]);
			}

			$indexes[$name]->addColumn($table->getColumn($colName));
		}
	}

	/**
	 * Loads the primary key for this table.
	 */
	protected function addPrimaryKey(Table $table)
	{
		$stmt = $this->dbh->query("SHOW KEYS FROM `" . $table->getName() . "`");

		// Loop through the returned results, grouping the same key_name together
		// adding each column for that key.
		while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			// Skip any non-primary keys.
			if ($row['Key_name'] !== 'PRIMARY') {
				continue;
			}
			$name = $row["Column_name"];
			$table->getColumn($name)->setPrimaryKey(true);
		}
	}

	/**
	 * Adds vendor-specific info for table.
	 *
	 * @param      Table $table
	 */
	protected function addTableVendorInfo(Table $table)
	{
		$stmt = $this->dbh->query("SHOW TABLE STATUS LIKE " . $this->dbh->quote($table->getName()));
		$row = $stmt->fetch(PDO::FETCH_ASSOC);
		$vi = $this->getNewVendorInfoObject($row);
		$table->addVendorInfo($vi);
	}
}