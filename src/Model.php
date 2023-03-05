<?php

namespace Wind\Db;

use ArrayAccess;
use ArrayIterator;
use IteratorAggregate;
use JsonSerializable;
use Traversable;
use Wind\Db\ModelQuery;

use function Amp\call;

/**
 * Database Model
 */
class Model implements ArrayAccess, IteratorAggregate, JsonSerializable
{

    const CONNECTION = null;
    const TABLE = null;
    const PRIMARY_KEY = 'id';

    private $isNew = false;

    private $dirtyAttributes = [];
    private $changedAttributes = [];
    private $attributes = [];

    public function offsetExists($name): bool {
        return isset($this->attributes[$name]) || isset($this->dirtyAttributes[$name]);
    }

    public function offsetGet($name): mixed {
        return $this->dirtyAttributes[$name] ?? $this->attributes[$name];
    }

    public function offsetSet($name, $value): void {
        $this->__set($name, $value);
    }

    public function offsetUnset($name): void {
        $this->__set($name, null);
    }

    public function getIterator(): Traversable {
        return new ArrayIterator($this->getAttributes());
    }

    public function jsonSerialize(): mixed {
        return $this->getAttributes();
    }

    public function getAttributes()
    {
        return array_merge($this->attributes, $this->dirtyAttributes);
    }

    public static function query()
    {
        return ModelQuery::create(static::class, static::CONNECTION);
    }

    public static function table()
    {
        return static::TABLE ?: self::uncamelize(substr(strrchr(static::class, '\\'), 1));
    }

    /**
     * 获得 Model 所在数据库的原始连接
     * @return \Wind\Db\Connection
     */
    public static function connection()
    {
        return static::CONNECTION !== null ? Db::connection(static::CONNECTION) : Db::connection();
    }

    /**
     * @param array $attributes
     */
    public function __construct($attributes, $isNew=true)
    {
        $this->attributes = $attributes;
        $this->isNew = $isNew;
    }

    /**
    * 驼峰命名转下划线命名
    *
    * @param string $camelCaps
    * @return string
    */
    private static function uncamelize($camelCaps) {
       return strtolower(preg_replace('/([a-z])([A-Z])/', '$1_$2', $camelCaps));
    }

    public function __get($name)
    {
        if (array_key_exists($name, $this->dirtyAttributes)) {
            return $this->dirtyAttributes[$name];
        } elseif (array_key_exists($name, $this->attributes)) {
            return $this->attributes[$name];
        }

        $getter = 'get'.ucfirst($name);

        if (method_exists($this, $getter)) {
            return $this->$getter();
        } else {
            throw new \Exception(sprintf('Undefined property: %s::$%s', static::class, $name));
        }
    }

    public function __set($name, $value)
    {
        $setter = 'set'.ucfirst($name);

        if (method_exists($this, $setter)) {
            $this->$setter($value);
        } elseif (!isset($this->attributes[$name]) || $this->attributes[$name] !== $value) {
            $this->dirtyAttributes[$name] = $value;
        }
    }

    public function __isset($name)
    {
        return isset($this->attributes[$name]) || isset($this->dirtyAttributes[$name]) || method_exists($this, 'get'.ucfirst($name));
    }

    private function locate()
    {
        if (static::PRIMARY_KEY && isset($this->{static::PRIMARY_KEY})) {
            $condition = [static::PRIMARY_KEY=>$this->{static::PRIMARY_KEY}];
            return static::query()->where($condition);
        } else {
            throw new DbException('No primary key for '.static::class.' to operate.');
        }
    }

    private function mergeAttributeChanges()
    {
        $this->attributes = array_merge($this->attributes, $this->dirtyAttributes);
        $this->changedAttributes = array_merge($this->changedAttributes, $this->dirtyAttributes);
        $this->dirtyAttributes = [];
    }

    public function save()
    {
        return call(function() {
            if ($this->isNew) {
                return $this->insert();
            } else {
                if ($this->dirtyAttributes) {
                    yield $this->locate()->update($this->dirtyAttributes);
                }
            }
        });
    }

    public function insert()
    {
        return call(function() {
            $id = yield self::query()->insert($this->dirtyAttributes);
            if ($id) {
                $this->attributes[static::PRIMARY_KEY] = $id;
            }
            $this->mergeAttributeChanges();
            return $id;
        });
    }

    public function update()
    {
        return call(function() {
            if ($this->dirtyAttributes) {
                yield $this->locate()->update($this->dirtyAttributes);
                $this->mergeAttributeChanges();
                return true;
            } else {
                return false;
            }
        });
    }

    public function updateCounters($counters, $withAttributes=[])
    {
        $update = [];

        foreach ($counters as $key => $n) {
            $update[$key] = new Expression($key.($n >= 0 ? '+' : '-').abs($n));
            //Todo: quoteKeys() is private in QueryBuilder, can't call here.
            // $update[$key] = new Expression($this->quoteKeys($key, true).($n >= 0 ? '+' : '-').abs($n));
        }

        $withAttributes && $update += $withAttributes;

        return $this->locate()->update($update);
    }

    public function delete()
    {
        return $this->locate()->delete();
    }

    public function beforeCreate()
    {}

    public function afterCreate()
    {}

    public function beforeUpdate()
    {}

    public function afterUpdate()
    {}

    public function beforeDelete()
    {}

    public function afterDelete()
    {}

}
