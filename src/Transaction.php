<?php

namespace Wind\Db;

use Amp\Sql\Transaction as SqlTransaction;

/**
 * Wind Db Transaction
 *
 * @mixin SqlTransaction
 */
class Transaction extends Executor {

    /**
     * @var SqlTransaction
     */
    protected $conn;

    public function __construct(SqlTransaction $transaction)
    {
        $this->conn = $transaction;
    }

    public function getIsolationLevel(): int
    {
        return $this->conn->getIsolationLevel();
    }

    /**
     * @return bool True if the transaction is active, false if it has been committed or rolled back.
     */
    public function isActive(): bool
    {
        return $this->conn->isActive();
    }

    /**
     * Commits the transaction and makes it inactive.
     *
     * @return void
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit()
    {
        $this->conn->commit();
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @return void
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback()
    {
        $this->conn->rollback();
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return void
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function createSavepoint(string $identifier)
    {
        $this->conn->createSavepoint($identifier);
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     * @return void
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier)
    {
        $this->conn->rollbackTo($identifier);
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     * @return void
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function releaseSavepoint(string $identifier)
    {
        $this->conn->releaseSavepoint($identifier);
    }

}
