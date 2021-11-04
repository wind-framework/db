<?php

namespace Wind\Db;

use Amp\Promise;
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
     * @return Promise<CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function commit(): Promise
    {
        return $this->conn->commit();
    }

    /**
     * Rolls back the transaction and makes it inactive.
     *
     * @return Promise<CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollback(): Promise
    {
        return $this->conn->rollback();
    }

    /**
     * Creates a savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function createSavepoint(string $identifier): Promise
    {
        return $this->conn->createSavepoint($identifier);
    }

    /**
     * Rolls back to the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function rollbackTo(string $identifier): Promise
    {
        return $this->conn->rollbackTo($identifier);
    }

    /**
     * Releases the savepoint with the given identifier.
     *
     * @param string $identifier Savepoint identifier.
     *
     * @return Promise<CommandResult>
     *
     * @throws TransactionError If the transaction has been committed or rolled back.
     */
    public function releaseSavepoint(string $identifier): Promise
    {
        return $this->conn->releaseSavepoint($identifier);
    }

}
