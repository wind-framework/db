<?php

namespace Wind\Db;

use Amp\Promise;

use function Amp\call;

/**
 * Query Builder for Model
 */
class ModelQuery extends QueryBuilder
{

    protected $modelClass;

    /**
     * @return self
     */
    public static function create($modelClass, $connection)
    {
        $connection = $connection !== null ? Db::connection($connection) : Db::connection();
        $query = (new self($connection))->from($modelClass::table());
        $query->modelClass = $modelClass;
        return $query;
    }

	/**
	 * 查询一条数据出来
     *
	 * @param array $params
	 * @return Promise<Model|null>
	 */
	public function fetchOne(array $params=[]): Promise {
		return call(function() use ($params) {
            $data = yield $this->connection->fetchOne($this->buildSelect(), $params);
            if ($data) {
                return $this->instanceModel($data);
            } else {
                return null;
            }
		});
	}

	/**
	 * 查询出全部数据
	 *
	 * @param string $sql
	 * @return Promise<array>
	 */
	public function fetchAll(array $params=[]): Promise {
		return call(function() use ($params) {
			$result = yield $this->query($this->buildSelect(), $params);
            $indexBy = $this->builder['index_by'] ?? null;

			$rows = [];

			while (yield $result->advance()) {
				$row = $result->getCurrent();
                $model = $this->instanceModel($row);
				if (!$indexBy) {
                    $rows[] = $model;
                } else {
                    if (!isset($row[$indexBy])) {
                        throw new DbException("Undefined indexBy key '{$indexBy}'.");
                    }
                    $rows[$row[$indexBy]] = $model;
                }
			}

            return $rows;
		});
	}

    /**
     * @return Model
     */
    private function instanceModel($data)
    {
        $ref = new \ReflectionClass($this->modelClass);
        return $ref->newInstance($data, false);
    }

}
