<?php

namespace Wind\Db;

use Amp\Mysql\MysqlConfig;
use Amp\Mysql\MysqlConnectionPool;
use Amp\Mysql\MysqlResult;
use Amp\Sql\Common\ConnectionPool;
use Amp\Sql\QueryError as SqlQueryError;
use Amp\Sql\SqlException;
use Psr\EventDispatcher\EventDispatcherInterface;
use Wind\Base\Config;
use Wind\Db\Event\QueryError;
use Wind\Db\Event\QueryEvent;

/**
 * Database base Connection and Fetch
 * @package Wind\Db
 */
class Connection
{

	/**
	 * @var MysqlConnectionPool
	 */
	private $pool;

	private $name;

	private $prefix = '';

    /**
     * Set fetchAll() return array index is used by special result key
     *
     * @var string
     */
    protected $indexBy;

	public function __construct($name) {
        $config = di()->get(Config::class)->get('database.'.$name);

		if (!$config) {
			throw new \Exception("Unable to find database config '{$name}'.");
		}

		//初始化数据库连接池
        $conn = new MysqlConfig(
            $config['host'],
            $config['port'],
            $config['username'],
            $config['password'],
            $config['database']
        );

		if (isset($config['charset'])) {
            $conn = $conn->withCharset($config['charset'], $config['collation']);
        }

		$maxConnection = $config['pool']['max_connections'] ?? ConnectionPool::DEFAULT_MAX_CONNECTIONS;
		$maxIdleTime = $config['pool']['max_idle_time'] ?? ConnectionPool::DEFAULT_IDLE_TIMEOUT;

		$this->pool = new MysqlConnectionPool($conn, $maxConnection, $maxIdleTime);
		$this->name = $name;
		$this->prefix = $config['prefix'];
	}

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
     * @return MysqlResult
     * @throws QueryException
     * @throws \Amp\Sql\QueryError
     */
	public function query(string $sql, array $params=[]): MysqlResult
	{
	    $eventDispatcher = di()->get(EventDispatcherInterface::class);
        $eventDispatcher->dispatch(new QueryEvent($sql));

        try {
            if ($params) {
                $statement = $this->pool->prepare($sql);
                return $statement->execute($params);
            } else {
                return $this->pool->query($sql);
            }
        } catch (SqlException|SqlQueryError $e) {
            $eventDispatcher->dispatch(new QueryError($sql, $e));
            throw new QueryException($e->getMessage(), $e->getCode(), $sql);
        }
	}

	/**
	 * @param string $sql
	 * @param array $params
	 * @return \Amp\Mysql\Result
	 * @throws QueryException
     * @throws \Amp\Sql\QueryError
	 */
	public function execute(string $sql, array $params = []): MysqlResult
	{
        $eventDispatcher = di()->get(EventDispatcherInterface::class);
        $eventDispatcher->dispatch(new QueryEvent($sql));

        try {
            return $this->pool->execute($sql, $params);
        } catch (SqlException|SqlQueryError $e) {
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
        $result = $this->query($sql, $params);
        foreach ($result as $row) {
            return $row;
        }
        return null;
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
