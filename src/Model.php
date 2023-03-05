<?php

namespace Wind\Db;

use ArrayAccess;
use ArrayIterator;
use IteratorAggregate;
use JsonSerializable;
use Traversable;
use Wind\Db\ModelQuery;

/**
 * Database Model
 */
class Model implements ArrayAccess, IteratorAggregate, JsonSerializable
{

    const CONNECTION = null;
    const TABLE = null;
    const PRIMARY_KEY = 'id';

    private $stored = false;

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
        return new ArrayIterator($this->getCurrentArray());
    }

    public function jsonSerialize(): mixed {
        return $this->getCurrentArray();
    }

    public function getCurrentArray()
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
    public function __construct($attributes)
    {
        $this->attributes = $attributes;
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
        $this->dirtyAttributes[$name] = $value;
    }

}
