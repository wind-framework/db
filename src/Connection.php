<?php

namespace Wind\Db;

use Amp\Mysql\ConnectionConfig;
use Amp\Sql\Common\ConnectionPool;
use Wind\Base\Config;

use function Amp\Mysql\pool;

/**
 * Database base Connection and Fetch
 * @package Wind\Db
 */
class Connection extends Executor
{

	private $name;
    private $type;

    /**
     * Connection Pool
     *
     * @var \Amp\Sql\Common\ConnectionPool
     */
    protected $conn;

	public function __construct($name) {
        $config = di()->get(Config::class)->get('database.'.$name);

		if (!$config) {
			throw new \Exception("Unable to find database config '{$name}'.");
		}

		//初始化数据库连接池
        $conn = new ConnectionConfig(
            $config['host'],
            $config['port'],
            $config['username'],
            $config['password'],
            $config['database']
        );

		if (isset($config['charset'])) {
            $conn->withCharset($config['charset'], $config['collation']);
        }

		$maxConnection = $config['pool']['max_connections'] ?? ConnectionPool::DEFAULT_MAX_CONNECTIONS;
		$maxIdleTime = $config['pool']['max_idle_time'] ?? ConnectionPool::DEFAULT_IDLE_TIMEOUT;

		$this->conn = pool($conn, $maxConnection, $maxIdleTime);
		$this->name = $name;
        $this->type = $config['type'];
		$this->prefix = $config['prefix'];
	}

    /**
     * Start a transaction
     *
     * @return Transaction
     */
    public function beginTransaction()
    {
        $connection = $this->conn->beginTransaction();
        return new Transaction($connection);
    }

    public function getType()
    {
        return $this->type;
    }

}
