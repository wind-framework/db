<?php

namespace Wind\Db;

use Amp\Mysql\MysqlResult;
use Amp\Mysql\Result;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Sql\QueryError as SqlQueryError;
use Amp\Sql\SqlException;
use Psr\EventDispatcher\EventDispatcherInterface;
use Wind\Db\Event\QueryError;
use Wind\Db\Event\QueryEvent;

abstract class Executor
{

    /**
     * Connection table prefix
     *
     * @var string
     */
	protected $prefix = '';

    /**
     * Set fetchAll() return array index is used by special result key
     *
     * @var string
     */
    protected $indexBy;

    /**
     * Database Executor & Connection Pool
     *
     * @var \Amp\Sql\Executor|\Amp\Sql\Common\ConnectionPool
     */
    protected $conn;

    /**
     * Get table name with prefix
     *
     * @param string $table Table name with non-prefix
     * @return string
     */
	public function prefix($table='')
    {
        return $table ? $this->prefix.$table : $this->prefix;
    }

    /**
     * Construct a query build from table
     *
     * @param string $name
     * @return QueryBuilder
     */
    public function table($name)
    {
        return (new QueryBuilder($this))->from($name);
    }

    /**
     * @param string $sql
     * @param array $params
     * @throws QueryException
     * @throws \Amp\Sql\QueryError
     */
	public function query(string $sql, array $params=[]): MysqlResult
	{
	    $eventDispatcher = di()->get(EventDispatcherInterface::class);
        $eventDispatcher->dispatch(new QueryEvent($sql));

        try {
            if ($params) {
                $statement = $this->conn->prepare($sql);
                return $statement->execute($params);
            } else {
                //https://github.com/amphp/mysql/issues/114
                if ($this->conn instanceof \Amp\Sql\Transaction) {
                    return $this->conn->execute($sql);
                } else {
                    return $this->conn->query($sql);
                }
            }
        } catch (ConnectionException|SqlException|SqlQueryError $e) {
            $eventDispatcher->dispatch(new QueryError($sql, $e));
            throw new QueryException($e->getMessage(), $e->getCode(), $sql);
        }
	}

	/**
	 * @param string $sql
	 * @param array $params
	 * @throws QueryException
     * @throws \Amp\Sql\QueryError
	 */
	public function execute(string $sql, array $params = []): MysqlResult
	{
        $eventDispatcher = di()->get(EventDispatcherInterface::class);
        $eventDispatcher->dispatch(new QueryEvent($sql));

        try {
            return $this->conn->execute($sql, $params);
        } catch (ConnectionException|SqlException|SqlQueryError $e) {
            $eventDispatcher->dispatch(new QueryError($sql, $e));
            throw new QueryException($e->getMessage(), $e->getCode(), $sql);
        }
	}

	/**
	 * 查询一条数据出来
	 *
	 * @param string $sql
	 * @param array $params
	 * @return array|null
	 */
	public function fetchOne($sql, array $params=[]): ?array {
        return $this->query($sql, $params)->fetchRow();
	}

	/**
	 * 查询出全部数据
	 *
	 * @param string $sql
	 * @param array $params
	 * @return array
	 */
	public function fetchAll($sql, array $params=[]): array {
        $result = $this->query($sql, $params);

        $rows = [];

        foreach ($result as $row) {
            if (!$this->indexBy) {
                $rows[] = $row;
            } else {
                if (!isset($row[$this->indexBy])) {
                    throw new DbException("Undefined indexBy key '{$this->indexBy}'.");
                }
                $rows[$row[$this->indexBy]] = $row;
            }
        }

        $this->indexBy = null;

        return $rows;
	}

    /**
     * Set key for fetchAll() return array
     *
     * @param string $key
     * @return $this
     */
    public function indexBy($key)
    {
        $this->indexBy = $key;
        return $this;
    }

    /**
     * Fetch column from all rows
     *
     * @param $sql
     * @param array $params
     * @param int $col
     * @return array
     */
    public function fetchColumn($sql, array $params=[], $col=0): array {
        $cols = [];
        $result = $this->query($sql, $params);

        foreach ($result as $row) {
            is_int($col) && $row = array_values($row);
            if ($this->indexBy) {
                $cols[$row[$this->indexBy]] = $row[$col];
            } else {
                $cols[] = $row[$col];
            }
        }

        $this->indexBy = null;

        return $cols;
    }

}
