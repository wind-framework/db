<?php
/**
 * Wind Framework QueryBuilder
 *
 * @author Pader <ypnow@163.com>
 */
namespace Wind\Db;

/**
 * QueryBuilder
 *
 * @psalm-type TJoin = array{type:string, table:string, compopr:string|array, alias:string|null}
 *
 * @psalm-type TBuilder = array{
 *     select?: string|string[],
 *     select_quote?: bool,
 *     alias?: string,
 *     join?: TJoin[],
 *     union?: string,
 *     where?: string|array<array-key, string|array>,
 *     where_params?: array,
 *     limit?: int,
 *     offset?: int,
 *     index_by?: string,
 *     having?: string|array<array-key, string|array>,
 *     group_by?: string|string[],
 *     order_by?: string|array<string, 3|4|string>
 * }
 */
class QueryBuilder {

    /**
     * @var Executor
     */
    protected $connection;

    /**
     * @var TBuilder
     */
	protected $builder = [];

    /** @var string */
	protected $table;

	public function __construct(Executor $connection)
	{
	    $this->connection = $connection;
	}

    /**
     * Select query fields
     *
     * @param string|string[] $fields
     * @param bool $quote
     */
	public function select($fields, $quote=true): static
	{
		$this->builder['select'] = $fields;
		$this->builder['select_quote'] = $quote;
		return $this;
	}

    /**
     * Select from table
     *
     * @param string|string[] $table
     * @param string|null $alias
     */
    public function from($table, $alias=null): static
    {
        $this->table = $this->quoteTable($table);
        return $alias ? $this->alias($alias) : $this;
    }

    /**
     * Set alias of table name
     *
     * It means `AS` of SQL
     *
     * @param string $alias
     */
	public function alias($alias): static
	{
		$this->builder['alias'] = $alias;
		return $this;
	}

    /**
     * Join table
     *
     * @param string $table
     * @param string|array $compopr
     * @param string $type One of "LEFT"|"RIGHT"|"INNER"|"OUTER"|""
     * @param string|null $alias
     */
	public function join($table, $compopr, $type='', $alias=null): static
	{
		$type = strtoupper($type);
		$this->builder['join'][] = compact('type', 'table', 'compopr', 'alias');
		return $this;
	}

	/**
     * Left join
     * @psalm-suppress MissingParamType
     * @psalm-suppress MissingReturnType
     */
	public function leftJoin($table, $compopr, $alias=null) { return $this->join($table, $compopr, 'left', $alias); }

    /**
     * Right join
     * @psalm-suppress MissingParamType
     * @psalm-suppress MissingReturnType
     */
	public function rightJoin($table, $compopr, $alias=null) { return $this->join($table, $compopr, 'right', $alias); }

    /**
     * Inner join
     * @psalm-suppress MissingParamType
     * @psalm-suppress MissingReturnType
     */
	public function innerJoin($table, $compopr, $alias=null) { return $this->join($table, $compopr, 'inner', $alias); }

    /**
     * Outer join
     * @psalm-suppress MissingParamType
     * @psalm-suppress MissingReturnType
     */
	public function outerJoin($table, $compopr, $alias=null) { return $this->join($table, $compopr, 'outer', $alias); }

    /**
     * Union the next select
     *
     * @param bool $all Is union all
     */
	public function union($all=false): static
	{
		$sql = $this->buildSelect();
		$this->builder['union'] = $sql.($all ? ' UNION ALL ' : ' UNION ');
		return $this;
	}

	/**
	 * Set where conditions.
	 *
	 * @param array<string, string|array>|string $cond Complex where condition array or string.
	 *
	 * Simple condition:
	 * ['id'=>100]
	 * means:
	 * `id`=100
	 *
	 * First element is 'AND', 'OR' mean condition connect method:
	 * ['name'=>'hello', 'nick'=>'world'] -> `name`='hello' AND `nick`='world'
	 * ['OR', 'name'=>'hello', 'nick'=>'world'] -> `name`='hello' OR `nick`='world'
	 *
	 * AND, OR support multiple nested:
	 * ['name'=>'hello', ['OR', 'c'=>1, 'd'=>2]] -> `name`='hello' AND (`c`=1 OR `d`=2)
	 *
	 * IN, NOT IN:
	 * ['name'=>['a', 'b', 'c']] -> `name` IN('a', 'b', 'c') AND
	 * ['name !'=>['a', 'b']] -> `name` NOT IN('a', 'b')
	 *
	 * BETWEEN:
	 * ['id BETWEEN'=>[100, 999]] -> `id` BETWEEN 100 AND 999
	 *
	 * Other symbols:
	 * =, !=, >, >=, <, <=, EXISTS, NOT EXISTS and others
	 * ['id >='=>100, 'live EXISTS'=>'system'] -> `id`>=100 AND `live` EXISTS ('system')
	 *
	 * @param array|null $params Unsupported yet!
	 */
	public function where($cond, $params=null): static
	{
		$this->builder['where'] = $cond;

		if ($params !== null) {
			$this->builder['where_params'] = $params;
		}

		return $this;
	}

    /**
     * Set having conditions, like where
     *
	 * @param array<string, string|array>|string $having Complex where condition array or string.
     */
	public function having($having): static
	{
		$this->builder['having'] = $having;
		return $this;
	}

	/**
	 * @param string|string[] $groupBy
	 */
	public function groupBy($groupBy): static
	{
		$this->builder['group_by'] = $groupBy;
		return $this;
	}

	/**
     * Sort the result
     *
	 * @param array<string, 3|4|string>|string $order Key is field name, value is constant of SORT_ASC, SORT_DESC or string 'asc', 'desc'
	 */
	public function orderBy($order): static
	{
		$this->builder['order_by'] = $order;
		return $this;
	}

	/**
	 * Limit
	 *
	 * @param int $limit
	 * @param int $offset
	 */
	public function limit($limit, $offset=null): static
	{
		$this->builder['limit'] = $limit;
		$offset !== null && $this->builder['offset'] = $offset;
		return $this;
	}

    /**
     * Offset
     *
     * @param int $num
     */
	public function offset($num): static
	{
		$this->builder['offset'] = $num;
		return $this;
	}

	/**
	 * Set fetchAll, fetchColumn result array index
	 *
	 * @param string $key
	 * @return static
	 */
	public function indexBy($key): static
    {
		$this->builder['index_by'] = $key;
		return $this;
	}

	/**
	 * Build sql from query builder
	 *
	 * @return string SQL String
	 */
	public function buildSelect()
	{
		//if (!$this->table) {
		//	throw new DatabaseException('Query build error, No table were selected!', $this->di);
		//}

		$sql = '';

		//UNION
		if (isset($this->builder['union'])) {
			$sql .= $this->builder['union'];
		}

		//SELECT
		if (isset($this->builder['select'])) {
            /**
             * @psalm-suppress PossiblyUndefinedArrayOffset
             * @psalm-suppress PossiblyInvalidOperand
             */
			$sql .= 'SELECT '.
				($this->builder['select_quote']
					? $this->quoteKeys($this->builder['select'])
					: $this->builder['select']);
		} else {
			$sql .= 'SELECT *';
		}

		//FROM
		$sql .= $this->buildFrom().$this->buildJoin().$this->buildWhere().$this->buildGroupBy().$this->buildHaving()
			.$this->buildOrderBy().$this->buildLimit().$this->buildOffset();

		return $sql;
	}

	/**
	 * Build from query
	 *
	 * Have alias if setted.
	 *
	 * @see quoteTable()
	 * @return string
	 */
	protected function buildFrom()
	{
		if ($this->table) {
			$sql = " FROM {$this->table}";

			//AS
			if (isset($this->builder['alias'])) {
				$sql .= " `{$this->builder['alias']}`";
			}

			return $sql;
		}

		return '';
	}

    /**
     * @return string
     */
	protected function buildJoin()
	{
		if (isset($this->builder['join'])) {
			$sql = '';
			foreach ($this->builder['join'] as $join) {
				$sql .= ' '.$join['type'].' JOIN '.$this->quoteTable($join['alias'] ? $join['table'].' '.$join['alias'] : $join['table']);
				//only a field mean USING()
				if (is_string($join['compopr']) && preg_match('/^[^\(\)\=]+$/', $join['compopr'])) {
					$sql .= ' USING('.$this->quoteKeys($join['compopr'], true).')';
				} else {
					$sql .= ' ON '.$this->parseJoinCompopr($join['compopr']);
				}
			}
			return $sql;
		}

		return '';
	}

    /**
     * @param string|array $compopr
     * @return string
     */
	protected function parseJoinCompopr($compopr)
	{
		if (is_array($compopr)) {
			$cmps = [];

			if (key($compopr) === 0 && ($t = strtoupper($compopr[0])) && ($t == 'AND' || $t == 'OR')) {
				$join = $t;
				unset($compopr[0]);
			} else {
				$join = 'AND';
			}

			foreach ($compopr as $k => $v) {
				$poly = false;
				if (is_int($k)) {
					$str = $this->parseJoinCompopr($v);
					count($v) > 1 && $poly = true;
				} else {
					$str = $this->parseWhere($k, $v);
				}
				$cmps[] = $poly ? '('.$str.')' : $str;
			}

			return join(" $join ", $cmps);
		}

		list($lkey, $symbol, $rkey) = preg_split('/\s*([\=\!\<\>]+)\s*/', trim($compopr), 2, PREG_SPLIT_DELIM_CAPTURE);

		return $this->quoteKeys($lkey).$symbol.$this->quoteKeys($rkey);
	}

	protected function buildWhere(): string
	{
		if (isset($this->builder['where'])) {
			$where = $this->parseWhere($this->builder['where']);
			$sql = " WHERE $where";

			//if (isset($this->builder['where_params']) {
			//}

			return $sql;
		}

		return '';
	}

	protected function buildGroupBy(): string
	{
		return isset($this->builder['group_by']) ?
			' GROUP BY '.$this->quoteKeys($this->builder['group_by']) : '';
	}

	protected function buildHaving(): string
	{
		return isset($this->builder['having']) ?
			' HAVING '.$this->parseWhere($this->builder['having']) : '';
	}

	protected function buildOrderBy(): string
	{
		$sql = '';

		if (isset($this->builder['order_by'])) {
			$orderBy = $this->builder['order_by'];

			//convert order by string to array
			if (!is_array($orderBy)) {
				$orderBy = preg_replace('/\s+/', ' ', $orderBy);
				$arr = array_map('trim', explode(',', $orderBy));
				$orderBy = [];

				foreach ($arr as $ostr) {
					list($field, $sort) = explode(' ', $ostr);
					$orderBy[$field] = $sort;
				}
			}

			$order = '';

			foreach ($orderBy as $field => $sort) {
				$order != '' && $order .= ', ';

				if ($sort === SORT_ASC) {
					$sort = 'ASC';
				} elseif ($sort === SORT_DESC) {
					$sort = 'DESC';
				} else {
					$sort = strtoupper($sort);
				}

				$order .= $this->quoteKeys($field).' '.$sort;
			}

			$sql .= ' ORDER BY '.$order;
		}

		return $sql;
	}

	protected function buildLimit(): string
	{
		if (isset($this->builder['limit'])) {
			$sql = ' LIMIT ';

			if (isset($this->builder['offset'])) {
				$sql .= "{$this->builder['offset']},";
				unset($this->builder['offset']);
			}

			$sql .= $this->builder['limit'];

			return $sql;
		}

		return '';
	}

	protected function buildOffset(): string
	{
		return isset($this->builder['offset']) ? " OFFSET {$this->builder['offset']}" : '';
	}

	/**
	 * Convert Keys To SQL Format
	 *
	 * @param string|string[]|Expression $keys
	 * @param bool $single Is a single field in top level
	 * @return string
	 */
	protected function quoteKeys($keys, $single=false)
	{
		if (is_array($keys)) {
			$qk = array();
			foreach($keys as $key) {
				$qk[] = $this->quoteKeys($key, true);
			}
			return join(', ', $qk);
		}

        if ($keys instanceof Expression) {
            return (string)$keys;
        }

		if (!$single && str_contains($keys, ',')) {
			//split with comma[,] except in brackets[()] and single quots['']
			$xkeys = preg_split('/,(?![^(\']*[\)\'])/', $keys);

			if (!isset($xkeys[1]) || $xkeys[0] == $keys) {
				return $this->quoteKeys($keys, true);
			} else {
				return $this->quoteKeys($xkeys);
			}
		}

		$keys = $col = $this->trim($keys);
		$str = $pre = $func = $alias = $as = '';
		$quote = true;

		//as
		if (($asa = $this->splitAs($keys)) !== false) {
			list($col, $as, $alias) = $asa;
		}

		//used function, two [?] for 1 is not match after space, 2 is not only space in brackets
		if (preg_match('/^(\w+)\s*\(\s*(.+?)?\s*\)$/i', $col, $m)) {
			$func = strtoupper($m[1]);

			if (!isset($m[2])) {
				$col = '';
				$quote = false;
			} elseif (strpbrk($m[2], ',()') !== false) { //nested function
				$col = $this->quoteKeys($m[2]);
				$quote = false;
			} else {
				$col = $m[2];
			}
		}

		//has prefix
		if (str_contains($col, '.')) {
			list($pre, $col) = explode('.', $col);
			$pre = trim($pre, ' `');
		}

		//add prefix
		$pre && $str = "`$pre`.";

		//add quote
		($col != '*' && $quote && !ctype_digit($col)) && $col = "`$col`";

		//add function
		$func ? $str = "$func($str$col)" : $str .= $col;

		//add alias
		$as && $str .= "$as`$alias`";

		return $str;
	}

    /**
     * Quote table as SQL string
     *
     * @param string|string[] $table
     * @return string
     */
	protected function quoteTable($table)
	{
		if (is_array($table)) {
			$str = '';
			foreach ($table as $t) {
				$str != '' && $str .= ',';
				$str .= $this->quoteTable($t);
			}
			return $str;
		}

		if (str_contains($table, ',')) {
			return $this->quoteTable(explode(',', $table));
		}

		$table = $this->trim($table);
		$db = $as = $alias = '';

		//as alias
		if (($asa = $this->splitAs($table)) !== false) {
			list($table, $as, $alias) = $asa;
		}

		//remove space char, because may have " db .  table" situation
		$table = str_replace(' ', '', $table);

		if (str_contains($table, '.')) {
			list($db, $table) = explode('.', $table);
		}

		$prefix = $this->connection->prefix();
		if ($prefix && substr($table, 0, strlen($prefix)) != $prefix) {
            $table = $prefix.$table;
        }

		$str = '';

		$db && $str = "`$db`.";
		$str .= "`$table`";
		$as && $str .= "$as`$alias`";

		return $str;
	}

	/**
	 * Quote values to safe sql string
	 *
	 * @param mixed $values
	 * @return string Keys
	 */
	public function quoteValues($values)
	{
		if (is_array($values)) {
			$list = [];
			foreach($values as $k => $v) {
                $list[] = $this->kvExpress($k, $v)[1];
			}
			return join(', ', $list);
		} elseif ($values === null) {
            return 'NULL';
        } else {
			return $this->quote($values);
		}
	}

    /**
     * Get [real key name, quoted value]
     *
     * @param string|int $k
     * @param mixed $v
     * @return array
     * @psalm-return list{int|string, string}
     */
    protected function kvExpress($k, $v)
    {
        if ($k && is_string($k) && str_starts_with($k, '^')) {
            $k = substr($k, 1);
        } elseif ($v instanceof Expression) {
            $v = (string)$v;
        } elseif ($v === null) {
            $v = 'NULL';
        } else {
            $v = $this->quoteValues($v);
        }
        return [$k, $v];
    }


	/**
	 * Parse where condition to sql format
	 *
	 * @param array|string $where
	 * @param mixed $value
	 * @return string
	 */
	public function parseWhere($where, $value=null)
	{
		if (is_array($where)) {
			$sqlWhere = array();

			//first value is AND or OR to set poly method
			if (key($where) === 0 && ($t = strtoupper($where[0])) && ($t == 'AND' || $t == 'OR')) {
				$join = $t;
				unset($where[0]);
			} else {
				$join = 'AND';
			}

			foreach ($where as $key => $val) {
				$poly = false;

				if (is_int($key)) {
					if (is_array($val)) {
						$sql = $this->parseWhere($val);
						count($val) > 1 && $poly = true;
					} else {
						$sql = $val;
					}
				} else {
					$sql = $this->parseWhere($key, $val);
				}

                if ($sql == '') {
                    continue;
                }

				$sqlWhere[] = $poly ? '('.$sql.')' : $sql;
			}

			return join(" $join ", $sqlWhere);
		}

        if ($value === null) {
            return '';
        }

		$where = $this->trim($where);

		if (($pos = strpos($where, ' ')) === false) {
			$key = $where;
			$cond = '';
		} else {
			$key = substr($where, 0, $pos);
			$cond = strtoupper(substr($where, $pos+1));
		}

		$key = $this->quoteKeys($key);

		if ($cond == '' || $cond == '!') {
			if (is_array($value) && count($value) == 1) {
				$value = $value[0];
			}
			$cond = !is_array($value) ? ($cond == '' ? '=' : '!=') : ($cond == '' ? 'IN' : 'NOT IN');
		}

		switch ($cond) {
			case '=':
			case '!=':
			case '>':
			case '>=':
			case '<':
			case '<=':
			case '<>':
				return $key.$cond.$this->quote($value);

			case 'IN':
			case 'NOT IN':
				return "$key $cond(".$this->quoteValues($value).')';

			case 'EXISTS':
			case 'NOT EXISTS':
				return $key.' '.$cond.'('.$value.')';

			case 'BETWEEN':
				is_numeric($value[0]) || $value[0] = $this->quote($value[0]);
				is_numeric($value[1]) || $value[1] = $this->quote($value[1]);
				return "$key BETWEEN {$value[0]} AND {$value[1]}";

			default:
				if (preg_match('/^\w+$/i', $cond)) {
					$cond = ' '.$cond.' ';
				}
				return $key.$cond.$this->quoteValues($value);
		}
	}

    /**
     * @param string $string
     * @return string
     */
	public function quote($string) {
		return '\''.addslashes($string).'\'';
	}

    /**
     * Clear condition in builder
     */
    public function clear() {
        $this->builder = [];
    }

	/**
	 * Split the table/field alias string
	 *
	 * @param string $str
	 * @psalm-return array{0:string, 1:string, 2:string}|false
	 */
	protected function splitAs($str)
	{
		//as alias
		if (stripos($str,' AS ') !== false) {
			list($col, $alias) = explode(' AS ', str_replace(' as ', ' AS ', $str));
			$alias = trim($alias, ' ');
			$as = ' AS ';
		} elseif (preg_match('/[^\s]\s[\w\-]+$/i', $str)) {
			//sure it's alias name after space
			//like "field alias", not "some )" or any other string after space not a alias
			list($col, $alias) = preg_split('/\s(?=[\w\-]+$)/i', $str);
			$as = ' ';
		} else {
			return false;
		}

		return [$col, $as, $alias];
	}

    /**
     * @param string $str
     * @return string
     */
	protected function trim($str)
	{
		return preg_replace('/\s+/', ' ', trim($str, ' `'));
	}

	/**
	 * Insert data
	 *
	 * @param array $data
	 * @return int Last insert id
	 */
	public function insert(array $data)
	{
		return $this->insertCommand($data);
	}

	/**
	 * Insert data or ignore on duplicate key
	 *
	 * @param array $data
	 * @return int Last insert id, return 0 if ignored.
	 */
	public function insertIgnore(array $data)
	{
		return $this->insertCommand($data, 'INSERT', 'IGNORE');
	}

	/**
	 * Insert data or on duplicate key update
	 *
	 * @param array $data
	 * @param array $update Update key values on duplicate key
	 * @return int Last insert or updated id, return 0 if no changed.
	 */
	public function insertOrUpdate(array $data, array $update)
	{
		$after = 'ON DUPLICATE KEY UPDATE '.$this->buildSets($update);
		return $this->insertCommand($data, 'INSERT', 'INTO', $after);
	}

	/**
	 * Replace into data
	 *
	 * @param array $data
	 * @return int Last insert id
	 */
	public function replace(array $data)
	{
		return $this->insertCommand($data, 'REPLACE');
	}

	/**
	 * Insert command
	 *
	 * @param array $data
	 * @param string $cmd Insert command, INSERT or REPLACE
	 * @param string $mode Insert mode, INTO or IGNORE
	 * @param string $after Append sql
	 * @return int Last insert id
	 */
	private function insertCommand(array $data, $cmd='INSERT', $mode='INTO', $after=null)
	{
		$keys = $this->quoteKeys(array_keys($data));
		$values = $this->quoteValues($data);

		$sql = "$cmd $mode {$this->table}($keys) VALUES($values)";

		if ($after !== null) {
			$sql .= ' '.$after;
		}

        $result = $this->connection->execute($sql);
        return $result->getLastInsertId();
	}

	/**
	 * Delete data
	 *
	 * @return int Affected row count
	 */
	public function delete()
	{
        $sql = 'DELETE'.$this->buildFrom().$this->buildWhere().$this->buildOrderBy()
            .$this->buildLimit().$this->buildOffset();
        $this->builder = [];
        $result = $this->connection->execute($sql);
        return $result->getRowCount();
	}

	/**
	 * Update data
	 *
	 * @param array $data
	 * @return int Affected row count
	 */
	public function update(array $data): int
	{
		$sql = 'UPDATE '.$this->table.' SET '
			.$this->buildSets($data)
			.$this->buildWhere()
			.$this->buildOrderBy()
			.$this->buildLimit()
			.$this->buildOffset();

		$this->builder = [];

        $result = $this->connection->execute($sql);
        return $result->getRowCount();
	}

	/**
	 * Build SET section
	 *
	 * @param array $data
	 * @return string
	 */
	private function buildSets(array $data)
	{
		$sets = [];

		foreach($data as $k => $v) {
            [$k, $v] = $this->kvExpress($k, $v);
            $sets[] = sprintf('`%s`=%s', $k, $v);
		}

		return join(', ', $sets);
	}

	/**
	 * 查询一条数据出来
	 */
	public function fetchOne(): ?array {
        return $this->connection->fetchOne($this->buildSelect());
	}

	/**
	 * 查询出全部数据
	 */
	public function fetchAll(): array {
        $sql = $this->buildSelect();
        if (!isset($this->builder['index_by'])) {
            return $this->connection->fetchAll($sql);
        } else {
            return $this->connection->indexBy($this->builder['index_by'])->fetchAll($sql);
        }
	}

    /**
     * Fetch column from all rows
     *
     * @param int $col
     */
    public function fetchColumn($col=0): array {
        $sql = $this->buildSelect();
        if (!isset($this->builder['index_by'])) {
            return $this->connection->fetchColumn($sql, [], $col);
        } else {
            return $this->connection->indexBy($this->builder['index_by'])->fetchColumn($sql, [], $col);
        }
    }

	//public function distinct($field)
	//{
	//	$this->builder['select'] = "distinct($field)";
	//	return $this;
	//}

	/**
	 * Count rows
	 *
	 * @param string $field
     * @return int
	 */
	public function count($field='*')
	{
        //Count is just temporary select, need to resume select
        $builder = $this->popTempBuilder();

        $this->select("COUNT($field)", false);
        $count = $this->scalar();

        $this->resumeTempBuilder($builder);

        return (int)$count;
	}

	/**
	 * Return first column value in row
	 *
	 * @param int|string $col
	 * @return mixed
	 */
	public function scalar($col=0)
	{
	    $sql = $this->buildSelect();

        $row = $this->connection->fetchOne($sql);

        if ($row === null) {
            return null;
        }

        is_int($col) && $row = array_values($row);

        return $row[$col] ?? null;
	}

    /**
     * @return array
     */
    private function popTempBuilder()
    {
        $builder = [];
        foreach (['select', 'select_quote', 'limit', 'offset', 'index_by'] as $key) {
            if (isset($this->builder[$key])) {
                $builder[$key] = $this->builder[$key];
                unset($this->builder[$key]);
            }
        }
        return $builder;
    }

    /**
     * @param TBuilder $builder
     * @psalm-suppress MissingReturnType
     */
    private function resumeTempBuilder($builder)
    {
        foreach (['select', 'select_quote', 'limit', 'offset', 'index_by'] as $key) {
            if (isset($builder[$key])) {
                $this->builder[$key] = $builder[$key];
            }
        }
    }
}
