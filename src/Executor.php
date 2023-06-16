<?php

namespace Wind\Db;

use Amp\Promise;
use Amp\Sql\ConnectionException;
use Amp\Sql\FailureException;
use Amp\Sql\QueryError as SqlQueryError;
use Psr\EventDispatcher\EventDispatcherInterface;
use Wind\Db\Event\QueryError;
use Wind\Db\Event\QueryEvent;

use function Amp\call;

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
     * Database Executor
     *
     * @var \Amp\Sql\Executor
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
     * @return Promise<\Amp\Mysql\ResultSet>
     * @throws QueryException
     * @throws \Amp\Sql\QueryError
     */
	public function query(string $sql, array $params=[]): Promise
	{
	    $eventDispatcher = di()->get(EventDispatcherInterface::class);
        $eventDispatcher->dispatch(new QueryEvent($sql));

        return call(function() use ($sql, $params, $eventDispatcher) {
            try {
                if ($params) {
                    $statement = yield $this->conn->prepare($sql);
                    return yield $statement->execute($params);
                } else {
                    //https://github.com/amphp/mysql/issues/114
                    if ($this->conn instanceof \Amp\Sql\Transaction) {
                        return yield $this->conn->execute($sql);
                    } else {
                        return yield $this->conn->query($sql);
                    }
                }
            } catch (ConnectionException|FailureException|SqlQueryError $e) {
                $eventDispatcher->dispatch(new QueryError($sql, $e));
                throw new QueryException($e->getMessage(), $e->getCode(), $sql);
            }
        });
	}

	/**
	 * @param string $sql
	 * @param array $params
	 * @return Promise<\Amp\Mysql\CommandResult>
	 * @throws QueryException
     * @throws \Amp\Sql\QueryError
	 */
	public function execute(string $sql, array $params = []): Promise
	{
        $eventDispatcher = di()->get(EventDispatcherInterface::class);
        $eventDispatcher->dispatch(new QueryEvent($sql));

	    return call(function() use ($sql, $params, $eventDispatcher) {
	        try {
                return yield $this->conn->execute($sql, $params);
            } catch (ConnectionException|FailureException|SqlQueryError $e) {
                $eventDispatcher->dispatch(new QueryError($sql, $e));
                throw new QueryException($e->getMessage(), $e->getCode(), $sql);
            }
        });

	}

	/**
	 * 查询一条数据出来
	 *
	 * @param string $sql
	 * @param array $params
	 * @return Promise<array>
	 */
	public function fetchOne($sql, array $params=[]): Promise {
		return call(function() use ($sql, $params) {
			$result = yield $this->query($sql, $params);

			if (yield $result->advance()) {
				$row = $result->getCurrent();
				//必须持续调用 nextResultSet 或 advance 直到无数据为止
				//防止资源未释放时后面的查询建立新连接的问题
				//如果查询出的数据行数大于一条，则仍然可能出现此问题
				yield $result->nextResultSet();
				return $row;
			} else {
				return null;
			}
		});
	}

    /**
     * Set key for fetchAll() return array
     *
     * Usage:
     * $connection->indexBy('id')->fetchAll();
     * $connection->indexBy('id')->fetchColumn();
     *
     * @param string $key
     * @return static
     */
    public function indexBy($key)
    {
        $connection = clone $this;
        $connection->indexBy = $key;
        return $connection;
    }

	/**
	 * 查询出全部数据
	 *
	 * @param string $sql
	 * @param array $params
	 * @return Promise<array>
	 */
	public function fetchAll($sql, array $params=[]): Promise {
		return call(function() use ($sql, $params) {
			$result = yield $this->query($sql, $params);

			$rows = [];

			while (yield $result->advance()) {
				$row = $result->getCurrent();
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
		});
	}

    /**
     * Fetch column from all rows
     *
     * @param $sql
     * @param array $params
     * @param int $col
     * @return Promise
     */
    public function fetchColumn($sql, array $params=[], $col=0): Promise {
        return call(function() use ($sql, $params, $col) {
            $cols = [];
            $result = yield $this->query($sql, $params);

            while (yield $result->advance()) {
                $row = $result->getCurrent();
                is_int($col) && $row = array_values($row);
                if ($this->indexBy) {
                    $cols[$row[$this->indexBy]] = $row[$col];
                } else {
                    $cols[] = $row[$col];
                }
            }

            $this->indexBy = null;

            return $cols;
        });
    }

}
