<?php

namespace Wind\Db;

use ArrayAccess;
use ArrayIterator;
use IteratorAggregate;
use JsonSerializable;
use Traversable;
use Wind\Event\EventDispatcher;

/**
 * Database Model
 */
abstract class Model implements ArrayAccess, IteratorAggregate, JsonSerializable
{

    /** @var string|null */
    const CONNECTION = null;

    /** @var string|null */
    const TABLE = null;

    const PRIMARY_KEY = 'id';

    /**
     * Dirty attributes are fields that have been changed but not saved to database
     * @var array
     */
    private $dirtyAttributes = [];

    /**
     * Changed attributes are fields that have been changed and also saved to database
     * @var array
     */
    private $changedAttributes = [];

    private EventDispatcher $eventDispatcher;

    /**
     * @param array $attributes All fields queried from or saved into the database
     */
    public function __construct(
        public array $attributes=[],
        private bool $isNew=true
    )
    {}

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

    public function jsonSerialize(): array {
        return $this->getAttributes();
    }

    public function getAttributes()
    {
        return array_merge($this->attributes, $this->dirtyAttributes);
    }

    /**
     * Start a model query
     * @return ModelQuery<static>
     */
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
     * Find one by primary key
     * @return ?static
     */
    public static function find($id)
    {
        return static::query()->find($id);
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

    /**
     * Get fields that have been changed but not saved to database
     * @return array
     */
    public function getDirtyAttributes()
    {
        return $this->dirtyAttributes;
    }

    /**
     * Get fields that have been changed and also saved to database
     * @return array
     */
    public function getChangedAttributes()
    {
        return $this->changedAttributes;
    }

    public function save()
    {
        $this->dispatchEvent('beforeSave', $this->isNew);

        if ($this->isNew) {
            $this->insert();
        } else {
            if ($this->dirtyAttributes) {
                $this->locate()->update($this->dirtyAttributes);
            }
        }

        $this->dispatchEvent('afterSave', $this->isNew);
    }

    public function insert()
    {
        $this->dispatchEvent('beforeInsert', $this->isNew);

        $id = self::query()->insert($this->getAttributes());
        if ($id) {
            $this->attributes[static::PRIMARY_KEY] = $id;
        }

        $this->mergeAttributeChanges();

        $this->dispatchEvent('afterInsert', $this->isNew);

        return $id;
    }

    public function update()
    {
        $this->dispatchEvent('beforeUpdate');

        if ($this->dirtyAttributes) {
            $this->locate()->update($this->dirtyAttributes);
            $this->mergeAttributeChanges();

            $this->dispatchEvent('afterUpdate');

            return true;
        } else {
            return false;
        }
    }

    public function delete()
    {
        $this->dispatchEvent('beforeDelete');
        if ($this->locate()->delete() > 0) {
            $this->dispatchEvent('afterDelete');
        }
    }

    /**
     * Update counters
     *
     * @param array $counters Update counter field-value map, value is positive number means plus, value is negative number mean decrease.
     * @param array $withAttributes Update with other field-value map.
     * @return int Affected rows count
     * @throws DbException
     */
    public function updateCounters($counters, $withAttributes=[])
    {
        $update = [];

        foreach ($counters as $key => $n) {
            $update[$key] = new ModelCounter($n);
        }

        $withAttributes && $update += $withAttributes;

        //在触发 updateCounters 的 beforeUpdate 事件时，dirtyAttributes 是包含 ModelCounter 实例字段的
        $this->dirtyAttributes = $update;
        $this->dispatchEvent('beforeUpdate');

        $affected = $this->locate()->update($update);

        if ($affected > 0) {
            //在合并进 attributes 和 changedAttributes 时，不能把 ModelCounter 实例合并进去，因为这并不是真正的值
            $this->dirtyAttributes = $withAttributes;
            $this->mergeAttributeChanges();

            //在合并完值之后，如果模型原本存在 counters 中的字段，则对模型该属性递增或递减，确保在后续能获取到变化的值，
            //但是注意，这里递增和递减后的值仅用于表达有变化的一种预估，由于可能存在并发原因，并不能代表真实数据库中的更新后的值。
            //如果模型属性中原本就没有查出该字段，则 updateCounters 之后仍不会有该字段，因为除非查询一次，否则无法得知任何预估的值。
            foreach ($counters as $key => $n) {
                if (isset($this->attributes[$key])) {
                    $this->attributes[$key] += $n;
                    $this->changedAttributes[$key] = $this->attributes[$key];
                }
            }

            $this->dispatchEvent('afterUpdate');
        }

        return $affected;
    }

    public function increment($name, $value=1, $withAttributes=[])
    {
        return $this->updateCounters([$name=>$value], $withAttributes);
    }

    public function decrement($name, $value=1, $withAttributes=[])
    {
        return $this->updateCounters([$name=>-$value], $withAttributes);
    }

    protected function dispatchEvent($name, ...$args)
    {
        $this->$name(...$args);
    }

    public function beforeInsert()
    {}

    public function afterInsert()
    {}

    public function beforeUpdate()
    {}

    public function afterUpdate()
    {}

    public function beforeSave()
    {}

    public function afterSave()
    {}

    public function beforeDelete()
    {}

    public function afterDelete()
    {}

}
