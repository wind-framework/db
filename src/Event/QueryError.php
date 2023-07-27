<?php


namespace Wind\Db\Event;


class QueryError extends \Wind\Event\Event
{

    public $sql = '';
    public $exception;

    /**
     * QueryError constructor.
     * @param string $sql
     * @param \Exception|\Error $exception
     */
    public function __construct($sql, $exception)
    {
        $this->sql = $sql;
        $this->exception = $exception;
    }

    public function __toString()
    {
        return $this->sql."\r\n".fmtException($this->exception, config('max_stack_trace'));
    }

}
