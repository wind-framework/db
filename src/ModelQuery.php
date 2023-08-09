<?php

namespace Wind\Db;

/**
 * Query Builder for Model
 */
class ModelQuery extends QueryBuilder
{

    protected $modelClass;

    /**
     * @return self
     */
    public static function create($modelClass)
    {
        $connection = $modelClass::connection();
        $table = $modelClass::table();
        $query = (new self($connection))->from($table);
        $query->modelClass = $modelClass;
        return $query;
    }

	/**
	 * Fetch first model
     *
	 * @return Model|null
	 */
	public function fetchOne()
    {
        $data = $this->connection->fetchOne($this->buildSelect());
        if ($data) {
            return $this->instanceModel($data);
        } else {
            return null;
        }
	}

	/**
	 * Fetch all models as list
	 *
	 * @return Model[]
	 */
	public function fetchAll()
    {
        $result = $this->connection->query($this->buildSelect());
        $indexBy = $this->builder['index_by'] ?? null;

        $rows = [];

        foreach ($result as $row) {
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
	}

    /**
     * Find one by primary key
     * @return Model|null
     */
    public function find($id)
    {
        $primaryKey = $this->modelClass::property('primaryKey');
        if ($primaryKey) {
            $condition = [$primaryKey=>$id];
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
	 * @return int Affected row count
	 */
	public function update(array $data): int
	{
        foreach ($data as $key => $value) {
            if ($value instanceof ModelCounter) {
                $n = $value->get();
                $data[$key] = new Expression($this->quoteKeys($key, true).($n >= 0 ? '+' : '-').abs($n));
            }
        }
        return parent::update($data);
	}

}
