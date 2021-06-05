<?php

namespace Wind\Db;

use Throwable;

class QueryException extends \Exception
{

    public $sql = '';

    public function __construct($message = "", $code = 0, $sql='')
    {
        parent::__construct($message, $code);
        $this->sql = $sql;
    }

}