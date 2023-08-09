<?php

namespace Wind\Db;

use Wind\Utils\PhpUtil;

/**
 * Database Model
 *
 * @psalm-type EventName = "init"|"retrieved"|"beforeCreate"|"created"|"beforeUpdate"|"updated"|"beforeSave"|"saved"|"beforeDelete"|"deleted"
 */
abstract class Model implements \ArrayAccess, \IteratorAggregate, \JsonSerializable
{

    const EVENT_INIT = 'init';
    const EVENT_RETRIEVED = 'retrieved';
    const EVENT_BEFORE_CREATE = 'beforeCreate';
    const EVENT_CREATED = 'created';
    const EVENT_BEFORE_UPDATE = 'beforeUpdate';
    const EVENT_UPDATED = 'updated';
    const EVENT_BEFORE_SAVE = 'beforeSave';
    const EVENT_SAVED = 'saved';
    const EVENT_BEFORE_DELETE = 'beforeDelete';
    const EVENT_DELETED = 'deleted';

    /** Global event callbacks */
    private static $eventCallbacks = [];

    /** Booted traits */
    private static $booted = [];

    /** @var string|null */
    protected $connection = null;

    /** @var string */
    protected $table;

    /** @var string */
    protected $primaryKey = 'id';

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

    /**
     * @param array $attributes All fields queried from or saved into the database
     */
    public function __construct(
        public array $attributes=[],
        private bool $isNew=true
    )
    {
        if (!isset(self::$booted[static::class])) {
            self::$booted[static::class] = true;

            // boot traits
            self::bootTraits();

            // boot model
            static::boot();
        }

        $this->dispatchEvent(self::EVENT_INIT);
        !$isNew && $this->dispatchEvent(self::EVENT_RETRIEVED);
    }

    /**
     * Boot when model first initialized
     */
    protected static function boot()
    {}

    private static function bootTraits()
    {
        $traits = PhpUtil::getTraits(static::class);

        foreach ($traits as $trait) {
            $baseName = basename(str_replace('\\', '/', $trait));

            $method = 'boot'.$baseName;
            if (method_exists($trait, $method)) {
                static::$method();
            }

            $method = 'init'.$baseName;
            if (method_exists($trait, $method)) {
                static::on(self::EVENT_INIT, static fn($model) => $model->$method());
            }
        }
    }

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

    public function getIterator(): \Traversable {
        return new \ArrayIterator($this->getAttributes());
    }

    public function jsonSerialize(): array {
        return $this->getAttributes();
    }

    public function getAttributes()
    {
        return array_merge($this->attributes, $this->dirtyAttributes);
    }

    public static function query()
    {
        return ModelQuery::create(static::class);
    }

    public static function table()
    {
        return self::property('table') ?: self::uncamelize(substr(strrchr(static::class, '\\'), 1));
    }

    /**
     * 获得 Model 所在数据库的原始连接
     * @return \Wind\Db\Connection
     */
    public static function connection()
    {
        $connection = self::property('connection');
        return $connection !== null ? Db::connection($connection) : Db::connection();
    }

    /**
     * Find one by primary key
     * @return static|null
     */
    public static function find($id)
    {
        return static::query()->find($id);
    }

    /**
     * Get default property value of current model outside the instance
     *
     * @param string $name
     * @return mixed null if property not exists
     */
    public static function property($name)
    {
        $ref = new \ReflectionClass(static::class);
        if ($ref->hasProperty($name)) {
            return $ref->getProperty($name)->getDefaultValue();
        } else {
            return null;
        }
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
        if ($this->primaryKey && isset($this->{$this->primaryKey})) {
            $condition = [$this->primaryKey=>$this->{$this->primaryKey}];
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
        $this->dispatchEvent(self::EVENT_BEFORE_SAVE, $this->isNew);

        if ($this->isNew) {
            $this->insert();
        } else {
            if ($this->dirtyAttributes) {
                $this->locate()->update($this->dirtyAttributes);
            }
        }

        $this->dispatchEvent(self::EVENT_SAVED, $this->isNew);
    }

    public function insert()
    {
        $this->dispatchEvent(self::EVENT_BEFORE_CREATE);

        $id = self::query()->insert($this->getAttributes());
        if ($id) {
            $this->attributes[$this->primaryKey] = $id;
        }

        $this->mergeAttributeChanges();

        $this->dispatchEvent(self::EVENT_CREATED);

        return $id;
    }

    public function update()
    {
        $this->dispatchEvent(self::EVENT_BEFORE_UPDATE);

        if ($this->dirtyAttributes) {
            $this->locate()->update($this->dirtyAttributes);
            $this->mergeAttributeChanges();

            $this->dispatchEvent(self::EVENT_UPDATED);

            return true;
        } else {
            return false;
        }
    }

    public function delete()
    {
        $this->dispatchEvent(self::EVENT_BEFORE_DELETE);
        if ($this->locate()->delete() > 0) {
            $this->dispatchEvent(self::EVENT_DELETED);
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

        $this->dispatchEvent(self::EVENT_BEFORE_UPDATE);

        //beforeUpdate 事件可能会更改 dirtyAttributes 的值
        //此处只接受来自事件的更新，所以不会在 beforeUpdate 前直接将 dirtyAttributes 与 $update 合并
        if ($this->dirtyAttributes !== $update) {
            $update = $this->dirtyAttributes;
        }

        $affected = $this->locate()->update($update);

        //在合并进 attributes 和 changedAttributes 时，不能把 ModelCounter 实例合并进去，因为这并不是真正的值
        $this->dirtyAttributes = array_filter($this->dirtyAttributes, fn($value) => !$value instanceof ModelCounter);
        $this->mergeAttributeChanges();

        if ($affected > 0) {
            //在合并完值之后，如果模型原本存在 counters 中的字段，则对模型该属性递增或递减，确保在后续能获取到变化的值，
            //但是注意，这里递增和递减后的值仅用于表达有变化的一种预估，由于可能存在并发原因，并不能代表真实数据库中的更新后的值。
            //如果模型属性中原本就没有查出该字段，则 updateCounters 之后仍不会有该字段，因为除非查询一次，否则无法得知任何预估的值。
            foreach ($counters as $key => $n) {
                if (isset($this->attributes[$key])) {
                    $this->attributes[$key] += $n;
                    $this->changedAttributes[$key] = $this->attributes[$key];
                }
            }

            $this->dispatchEvent(self::EVENT_UPDATED);
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

    /**
     * Dispatch model event
     *
     * @param string $name Event name
     * @psalm-param EventName $name Event name
     * @param array $args Event arguments
     */
    private function dispatchEvent($name, ...$args)
    {
        $index = static::class.':'.$name;

        if (isset(self::$eventCallbacks[$index])) {
            foreach (self::$eventCallbacks[$index] as $callback) {
                $callback($this, ...$args);
            }
        }

        $this->$name(...$args);
    }

    /**
     * Add callback to event
     *
     * @param string $name Event name
     * @psalm-param EventName $name
     * @param callable $callback
     */
    public static function on($name, $callback)
    {
        $index = static::class.':'.$name;
        self::$eventCallbacks[$index] ??= [];
        self::$eventCallbacks[$index][] = $callback;
    }

    /**
     * Off the callback on event
     *
     * @param string $name Event name
     * @psalm-param EventName $name
     * @param ?callable $callback
     */
    public static function off($name, $callback=null)
    {
        $index = static::class.':'.$name;

        if (!isset(self::$eventCallbacks[$index])) {
            return;
        }

        if ($callback === null) {
            unset(self::$eventCallbacks[$index]);
        } elseif (in_array($callback, self::$eventCallbacks[$index], true)) {
            $i = array_search($callback, self::$eventCallbacks[$index], true);
            unset(self::$eventCallbacks[$index][$i]);
            if (count(self::$eventCallbacks[$index]) == 0) {
                unset(self::$eventCallbacks[$index]);
            }
        }
    }

    /**
     * Init when instance created
     */
    protected function init()
    {}

    /**
     * After item retrieved
     */
    protected function retrieved()
    {}

    /**
     * Before insert event
     */
    protected function beforeCreate()
    {}

    /**
     * After insert event
     */
    protected function created()
    {}

    /**
     * Before update event
     */
    protected function beforeUpdate()
    {}

    /**
     * After update event
     */
    protected function updated()
    {}

    /**
     * Before save event
     * @param bool $isNew
     */
    protected function beforeSave($isNew)
    {}

    /**
     * After save event
     * @param bool $isNew
     */
    protected function saved($isNew)
    {}

    /**
     * Before delete event
     */
    protected function beforeDelete()
    {}

    /**
     * Before delete event
     */
    protected function deleted()
    {}

}
