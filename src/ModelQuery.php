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
     * Find one by primary key
     * @return Promise<Model|null>
     */
    public function find($id)
    {
        if ($this->modelClass::PRIMARY_KEY) {
            $condition = [$this->modelClass::PRIMARY_KEY=>$id];
            return $this->where($condition)->fetchOne();
        } else {
            throw new DbException('No primary key for '.$this->modelClass.' to find.');
        }
    }

    /**
     * @return Model
     */
    private function instanceModel($data)
    {
        $ref = new \ReflectionClass($this->modelClass);
        return $ref->newInstance($data, false);
    }

	/**
	 * Update data
	 *
	 * @param array $data
	 * @return Promise<int> Affected row count
	 */
	public function update(array $data): Promise
	{
        foreach ($data as $key => $value) {
            if ($value instanceof ModelCounter) {
                $n = (int)$value->__toString();
                $data[$key] = new Expression($this->quoteKeys($key, true).($n >= 0 ? '+' : '-').abs($n));
            }
        }
        return parent::update($data);
	}

}
