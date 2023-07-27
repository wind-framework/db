<?php

namespace Wind\Db;

/**
 * Query Builder for Model
 *
 * @template T of Model
 */
class ModelQuery extends QueryBuilder
{

    /** @var class-string<T> */
    protected $modelClass;

    /**
     * @param class-string<T> Model class string
     * @return self<T>
     */
    public static function create($modelClass, $connection)
    {
        $connection = $connection !== null ? Db::connection($connection) : Db::connection();
        $query = (new self($connection))->from($modelClass::table());
        /** @psalm-suppress UndefinedPropertyAssignment */
        $query->modelClass = $modelClass;
        return $query;
    }

	/**
	 * Fetch first model
     *
     * @psalm-suppress LessSpecificImplementedReturnType
	 * @return ?T
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
	 * @return T[]
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
     * @return ?T
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
     * @return T
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
