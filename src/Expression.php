<?php

namespace Wind\Db;

/**
 * Wind Db Expression
 */
class Expression
{

    protected $value;

    /**
     * @param mixed $value
     */
    public function __construct($value)
    {
        $this->value = $value;
    }

    /**
     * Get original value
     * @return mixed
     */
    public function get()
    {
        return $this->value;
    }

    /**
     * Get expression string value
     *
     * @return string
     */
    public function __toString()
    {
        return (string)$this->value;
    }
}
