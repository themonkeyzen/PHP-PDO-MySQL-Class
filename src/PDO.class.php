<?php
/*
 * PHP-PDO-MySQL-Class
 * https://github.com/lincanbin/PHP-PDO-MySQL-Class
 *
 * Copyright 2015 Canbin Lin (lincanbin@hotmail.com)
 * http://www.94cb.com/
 *
 * Licensed under the Apache License, Version 2.0:
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * A PHP MySQL PDO class similar to the Python MySQLdb.
 */
require(__DIR__ . '/PDO.Log.class.php');
require(__DIR__ . '/PDO.Iterator.class.php');

enum DBDriver: string
{
    case MySQL = 'mysql';
    case PostgreSQL = 'pgsql';
}

class DB
{
	private ?PDO $pdo;
	private false|PDOStatement $sQuery;
	private bool $connectionStatus = false;
	private PDOLog $logObject;
	private array $parameters;
	public int $rowCount = 0;
	public int $columnCount = 0;
	public int $querycount = 0;


	private int $retryAttempt = 0;
	const AUTO_RECONNECT = true;
	const RETRY_ATTEMPTS = 3;

	public function __construct(
        private readonly string $Host,
        private readonly int    $DBPort,
        private readonly string $DBName,
        private readonly string $DBUser,
        private readonly string $DBPassword,
        private readonly DBDriver $DBDriver = DBDriver::MySQL
    ) {
		$this->logObject  = new PDOLog();
		$this->parameters = [];
		$this->Connect();
	}


    private function Connect(): void
    {
        try {
            $dsn = match ($this->DBDriver) {
                DBDriver::MySQL => "mysql:host={$this->Host};port={$this->DBPort};dbname={$this->DBName};charset=utf8;",
                DBDriver::PostgreSQL => "pgsql:host={$this->Host};port={$this->DBPort};dbname={$this->DBName};",
            };

            $this->pdo = new PDO(
                $dsn,
                $this->DBUser,
                $this->DBPassword,
                [
                    PDO::ATTR_EMULATE_PREPARES => false,
                    PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                    PDO::ATTR_PERSISTENT => false,
                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
                ]
            );

            $this->connectionStatus = true;

        } catch (PDOException $e) {
            $this->ExceptionLog($e, '', 'Connect');
        }
    }

	private function SetFailureFlag(): void
    {
		$this->pdo = null;
		$this->connectionStatus = false;
	}

    /**
     * close pdo connection
     */
	public function closeConnection(): void
    {
		$this->pdo = null;
	}

	private function Init(string $query, array $parameters = null, array $driverOptions = []): void
    {
		if (!$this->connectionStatus) {
			$this->Connect();
		}
		try {
			$this->parameters = $parameters;
			$this->sQuery     = $this->pdo->prepare($this->BuildParams($query, $this->parameters), $driverOptions);

			if (!empty($this->parameters)) {
				if (array_key_exists(0, $parameters)) {
					$parametersType = true;
					array_unshift($this->parameters, "");
					unset($this->parameters[0]);
				} else {
					$parametersType = false;
				}
				foreach ($this->parameters as $column => $value) {
					$this->sQuery->bindParam($parametersType ? intval($column) : ":" . $column, $this->parameters[$column]); //It would be query after loop end(before 'sQuery->execute()').It is wrong to use $value.
				}
			}

			if (!isset($driverOptions[PDO::ATTR_CURSOR])) {
                $this->sQuery->execute();
            }
			$this->querycount++;
		}
		catch (PDOException $e) {
			$this->ExceptionLog($e, $this->BuildParams($query), 'Init', ['query' => $query, 'parameters' => $parameters]);

		}

		$this->parameters = [];
	}

	private function BuildParams(string $query, array $params = null)
	{
		if (!empty($params)) {
			$array_parameter_found = false;
			foreach ($params as $parameter_key => $parameter) {
				if (is_array($parameter)){
					$array_parameter_found = true;
					$in = "";
					foreach ($parameter as $key => $value){
						$name_placeholder = $parameter_key."_".$key;
						// concatenates params as named placeholders
                            $in .= ":".$name_placeholder.", ";
						// adds each single parameter to $params
						$params[$name_placeholder] = $value;
					}
					$in = rtrim($in, ", ");
					$query = preg_replace("/:".$parameter_key."/", $in, $query);
					// removes array form $params
					unset($params[$parameter_key]);
				}
			}

			// updates $this->params if $params and $query have changed
			if ($array_parameter_found) $this->parameters = $params;
		}
		return $query;
	}

	public function beginTransaction(): bool
    {
		return $this->pdo->beginTransaction();
	}

	public function commit(): bool
    {
		return $this->pdo->commit();
	}

	public function rollBack(): bool
    {
		return $this->pdo->rollBack();
	}

	public function inTransaction(): bool
    {
		return $this->pdo->inTransaction();
	}

    /**
     * execute a sql query, returns an result array in the select operation, and returns the number of rows affected in other operations
     */
	public function query(string $query, array $params = null, int $fetchMode = PDO::FETCH_ASSOC)
	{
		$query        = trim($query);
		$rawStatement = preg_split("/( |\r|\n)/", $query);
		$this->Init($query, $params);
		$statement = strtolower($rawStatement[0]);
		if ($statement === 'select' || $statement === 'show' || $statement === 'call' || $statement === 'describe') {
			return $this->sQuery->fetchAll($fetchMode);
		} elseif ($statement === 'insert' || $statement === 'update' || $statement === 'delete') {
			return $this->sQuery->rowCount();
		} else {
			return NULL;
		}
	}

    /**
     * execute a sql query, returns an iterator in the select operation, and returns the number of rows affected in other operations
     */
    public function iterator(string $query, array $params = null, int $fetchMode = PDO::FETCH_ASSOC): int|null|PDOIterator
    {
        $query        = trim($query);
        $rawStatement = preg_split("/( |\r|\n)/", $query);
        $this->Init($query, $params, [PDO::ATTR_CURSOR => PDO::CURSOR_SCROLL]);
        $statement = strtolower(trim($rawStatement[0]));
        if ($statement === 'select' || $statement === 'show' || $statement === 'call' || $statement === 'describe') {
            return new PDOIterator($this->sQuery, $fetchMode);
        } elseif ($statement === 'insert' || $statement === 'update' || $statement === 'delete') {
            return $this->sQuery->rowCount();
        } else {
            return NULL;
        }
    }

	public function insert(string $tableName, array $params = null): bool|string
    {
		$keys = array_keys($params);
		$rowCount = $this->query(
			'INSERT INTO `' . $tableName . '` (`' . implode('`,`', $keys) . '`)
			VALUES (:' . implode(',:', $keys) . ')',
			$params
		);
		if ($rowCount === 0) {
			return false;
		}
		return $this->lastInsertId();
    }

    /**
     * insert multi rows
     *
     * @param string $tableName database table name
     * @param array $params structure like [[colname1 => value1, colname2 => value2], [colname1 => value3, colname2 => value4]]
     * @return boolean success or not
     */
    public function insertMulti(string $tableName, array $params = []): bool
    {
        $rowCount = 0;
        if (!empty($params)) {
            $insParaStr = '';
            $insValueArray = [];

            foreach ($params as $addRow) {
                $insColStr = implode('`,`', array_keys($addRow));
                $insParaStr .= '(' . implode(",", array_fill(0, count($addRow), "?")) . '),';
                $insValueArray = array_merge($insValueArray, array_values($addRow));
            }
            $insParaStr = substr($insParaStr, 0, -1);
            $dbQuery = "INSERT INTO {$tableName} (
                            `$insColStr`
                        ) VALUES
                            $insParaStr";
            $rowCount = $this->query($dbQuery, $insValueArray);
        }
        return (bool) ($rowCount > 0);
    }

    /**
     * update
     *
     * @param string $tableName
     * @param array $params
     * @param array $where
     * @return int affect rows
     */
    public function update(string $tableName, array $params = [], array $where = [])
    {
        $rowCount = 0;
        if (!empty($params)) {
            $updColStr = '';
            $whereStr = '';
            $updatePara = [];
            // Build update statement
            foreach ($params as $key => $value) {
                $updColStr .= "{$key}=?,";
            }
            $updColStr = substr($updColStr, 0, -1);
            $dbQuery = "UPDATE {$tableName}
                        SET {$updColStr}";
            // where condition
            if (is_array($where)) {
                foreach ($where as $key => $value) {
                    // Is there need to add "OR" condition?
                    $whereStr .= "AND {$key}=?";
                }
                $dbQuery .= " WHERE 1=1 {$whereStr}";
                $updatePara = array_merge(array_values($params), array_values($where));
            } else {
                $updatePara = array_values($params);
            }
            $rowCount = $this->query($dbQuery, $updatePara);
        }
        return $rowCount;
    }

	public function lastInsertId(): string
    {
		return $this->pdo->lastInsertId();
	}

	public function column(string $query, array $params = null): array
    {
		$this->Init($query, $params);
		$resultColumn = $this->sQuery->fetchAll(PDO::FETCH_COLUMN);
		$this->rowCount = $this->sQuery->rowCount();
		$this->columnCount = $this->sQuery->columnCount();
		$this->sQuery->closeCursor();
		return $resultColumn;
	}

	public function row(string $query, array $params = null, int $fetchmode = PDO::FETCH_ASSOC)
	{
		$this->Init($query, $params);
		$resultRow = $this->sQuery->fetch($fetchmode);
		$this->rowCount = $this->sQuery->rowCount();
		$this->columnCount = $this->sQuery->columnCount();
		$this->sQuery->closeCursor();
		return $resultRow;
	}

	public function single(string $query, array $params = null)
	{
		$this->Init($query, $params);
		return $this->sQuery->fetchColumn();
	}

    /**
     * @param PDOException $e
     * @param string $sql
     * @param string $method
     * @param array $parameters
     */
	private function ExceptionLog(PDOException $e, string $sql = "", string $method = '', array $parameters = []): void
    {
		$message = $e->getMessage();
		$exception = 'Unhandled Exception. <br />';
		$exception .= $message;
		$exception .= "<br /> You can find the error back in the log.";

		if (!empty($sql)) {
			$message .= "\r\nRaw SQL : " . $sql;
		}
		$this->logObject->write($message, $this->DBName . md5($this->DBPassword));
		if (
			self::AUTO_RECONNECT
			&& $this->retryAttempt < self::RETRY_ATTEMPTS
			&& stripos($message, 'server has gone away') !== false
			&& !empty($method)
			&& !$this->inTransaction()
		) {
			$this->SetFailureFlag();
			$this->retryAttempt ++;
			$this->logObject->write('Retry ' . $this->retryAttempt . ' times', $this->DBName . md5($this->DBPassword));
			call_user_func_array(array($this, $method), $parameters);
		} else {
			if (($this->pdo === null || !$this->inTransaction()) && php_sapi_name() !== "cli") {
				//Prevent search engines to crawl
				header("HTTP/1.1 500 Internal Server Error");
				header("Status: 500 Internal Server Error");
				echo $exception;
				exit();
			} else {
				throw $e;
			}
		}
	}
}
